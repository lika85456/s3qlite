import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";

import { connect } from "../src/connection";
import type { KV } from "../src/kv/kv";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

describe("concurrent sync queue", () => {
	it.scopedLive("queues concurrent writes during sync so they survive", () =>
		Effect.gen(function* () {
			const dbName = `test-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const localKV1 = yield* makeMemoryKV();
			const localKV2 = yield* makeMemoryKV();

			// Instance 1: create table + initial row, push
			const instance1 = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKV1),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* Effect.tryPromise(() =>
				instance1.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"),
			);
			yield* Effect.tryPromise(() =>
				instance1.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
			);
			yield* instance1.push();

			// Instance 2: pulls, inserts its own row, pushes → base snapshot covers its batch
			const instance2 = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKV2),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* Effect.tryPromise(() =>
				instance2.run("INSERT INTO users (id, name) VALUES (?, ?)", "2", "bob"),
			);
			yield* instance2.push();

			// Instance 1 pulls to get instance2's changes, then pushes a new row
			yield* instance1.pull();
			yield* Effect.tryPromise(() =>
				instance1.run("INSERT INTO users (id, name) VALUES (?, ?)", "3", "carol"),
			);
			yield* instance1.push();

			// Close instance2 so we can reconnect with a blocking remote KV
			yield* Effect.tryPromise(() => instance2.close()).pipe(Effect.orDie);

			// Blocking remote KV: stalls readStream inside pullFiles, which runs concurrently
			// with extractBatch in Effect.all. extractBatch completes while pullFiles is blocked,
			// so a concurrent INSERT lands on the old connection and is lost.
			const syncBlocked = yield* Deferred.make<void>();
			const syncProceed = yield* Deferred.make<void>();
			let firstCall = true;

			const blockingRemoteKV: KV = {
				...remoteKV,
				readStream: (key) => {
					if (firstCall && key.endsWith(".batch")) {
						firstCall = false;
						// oxlint-disable-next-line local-rules/no-null-undefined-option -- void Deferred
						return Deferred.succeed(syncBlocked, void 0).pipe(
							Effect.flatMap(() => Deferred.await(syncProceed)),
							Effect.flatMap(() => remoteKV.readStream(key)),
						);
					}
					return remoteKV.readStream(key);
				},
			};

			// Reconnect instance2 with blocking remote KV
			const instance2b = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, blockingRemoteKV),
					Effect.provideService(LocalKV, localKV2),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			// Fork sync — pullFiles will block inside readStream
			const syncFiber = yield* Effect.fork(instance2b.sync());

			// Wait for pullFiles to hit the block (inside Effect.all, concurrently with extractBatch)
			yield* Deferred.await(syncBlocked);

			// Yield to let extractBatch fiber complete before we write to the old connection
			yield* Effect.yieldNow();

			// Concurrent write while sync is in progress. It should wait behind sync and
			// run on the swapped live connection once sync finishes.
			const writeFiber = yield* Effect.fork(
				Effect.tryPromise(() =>
					instance2b.run("INSERT INTO users (id, name) VALUES (?, ?)", "4", "dave"),
				),
			);

			// Release pullFiles so pull can continue and swap to the new connection
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- void Deferred
			yield* Deferred.succeed(syncProceed, void 0);
			yield* Fiber.join(syncFiber);
			yield* Fiber.join(writeFiber);

			// The queued write must survive the sync.
			const users = yield* Effect.tryPromise(
				() =>
					instance2b.all("SELECT id, name FROM users ORDER BY id") as Promise<
						{ id: string; name: string }[]
					>,
			);

			expect(users).toEqual([
				{ id: "1", name: "alice" },
				{ id: "2", name: "bob" },
				{ id: "3", name: "carol" },
				{ id: "4", name: "dave" },
			]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});