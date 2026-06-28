import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

describe("s3qlite sync", () => {
	it.scopedLive("runs basic sync between two connections", () =>
		Effect.gen(function* () {
			const dbName = `test-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const firstLocalKV = yield* makeMemoryKV();
			const secondLocalKV = yield* makeMemoryKV();

			const first = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, firstLocalKV),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* Effect.tryPromise(() =>
				first.exec("CREATE TABLE users (id TEXT PRIMARY KEY, username TEXT NOT NULL)"),
			);
			yield* Effect.tryPromise(() =>
				first.run("INSERT INTO users (id, username) VALUES (?, ?)", "1", "alice"),
			);
			yield* first.push();

			const second = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, secondLocalKV),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* second.pull();
			yield* Effect.tryPromise(() =>
				second.run("INSERT INTO users (id, username) VALUES (?, ?)", "2", "bob"),
			);
			yield* second.push();

			yield* first.pull();

			const firstUsers = yield* Effect.tryPromise(
				() => first.all("SELECT id, username FROM users ORDER BY id") as Promise<unknown[]>,
			);
			const secondUsers = yield* Effect.tryPromise(
				() =>
					second.all("SELECT id, username FROM users ORDER BY id") as Promise<unknown[]>,
			);

			expect(firstUsers).toEqual([
				{ id: "1", username: "alice" },
				{ id: "2", username: "bob" },
			]);
			expect(secondUsers).toEqual([
				{ id: "1", username: "alice" },
				{ id: "2", username: "bob" },
			]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});
