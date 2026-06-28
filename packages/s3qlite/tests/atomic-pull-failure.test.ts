import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Option } from "effect";

import { connect } from "../src/connection";
import type { KV } from "../src/kv/kv";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

const schemaSql = "CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL)";

describe("atomic pull on failure", () => {
	it.scopedLive("local DB unchanged when remote batch download fails", () =>
		Effect.gen(function* () {
			const dbName = `atomic-${crypto.randomUUID().slice(0, 8)}`;
			const sharedRemoteKV = yield* makeMemoryKV();

			// Failable wrapper: returns None for .batch keys when enabled
			let failReadStream = false;
			const failableRemoteKV: KV = {
				...sharedRemoteKV,
				readStream: (key: string) => {
					if (failReadStream && key.endsWith(".batch")) {
						return Effect.succeed(Option.none());
					}
					return sharedRemoteKV.readStream(key);
				},
			};

			const localKVA = yield* makeMemoryKV();
			const localKVB = yield* makeMemoryKV();

			// Instance A seeds the database
			const a = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, failableRemoteKV),
					Effect.provideService(LocalKV, localKVA),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* Effect.tryPromise(() => a.exec(schemaSql));
			yield* Effect.tryPromise(() =>
				a.run("INSERT INTO items (id, value) VALUES (?, ?)", "1", "a-first"),
			);
			yield* a.push();

			// Instance B pulls initial data, adds local change
			const b = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, failableRemoteKV),
					Effect.provideService(LocalKV, localKVB),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* b.pull();
			yield* Effect.tryPromise(() =>
				b.run("INSERT INTO items (id, value) VALUES (?, ?)", "2", "b-local"),
			);

			// A pushes another change — creates remote batch for B
			yield* Effect.tryPromise(() =>
				a.run("INSERT INTO items (id, value) VALUES (?, ?)", "3", "a-second"),
			);
			yield* a.pull();
			yield* a.push();

			// Snapshot B's rows before the failing pull
			const rowsBefore = yield* Effect.tryPromise(
				() => b.all("SELECT id, value FROM items ORDER BY id") as Promise<unknown[]>,
			);

			// Inject failure: remote batch readStream returns None
			failReadStream = true;
			const pullResult = yield* Effect.either(b.pull());
			expect(pullResult._tag).toBe("Left");

			// Local DB must be identical to before the failed pull
			const rowsAfter = yield* Effect.tryPromise(
				() => b.all("SELECT id, value FROM items ORDER BY id") as Promise<unknown[]>,
			);
			expect(rowsAfter).toEqual(rowsBefore);

			// Subsequent healthy pull succeeds and gets the missing batch
			failReadStream = false;
			yield* b.pull();

			const rowsFinal = yield* Effect.tryPromise(
				() => b.all("SELECT id, value FROM items ORDER BY id") as Promise<unknown[]>,
			);
			expect(rowsFinal).toEqual([
				{ id: "1", value: "a-first" },
				{ id: "2", value: "b-local" },
				{ id: "3", value: "a-second" },
			]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});
