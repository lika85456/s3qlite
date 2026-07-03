import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

rustfs("last-push-wins", (it) => {
	it.effect("keeps the value from the instance that syncs last for the same primary key", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `last-push-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("last-push-wins");
				const localA = yield* makeMemoryKV();
				const localB = yield* makeMemoryKV();

				const seeder = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localA),
					),
					(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					seeder.exec(
						"CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
					),
				);
				yield* seeder.push();

				const a = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localA),
					),
					(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
				);
				const b = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localB),
					),
					(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					a.run("INSERT INTO messages (id, body) VALUES (?, ?)", 1, "from-a"),
				);
				yield* Effect.tryPromise(() =>
					b.run("INSERT INTO messages (id, body) VALUES (?, ?)", 1, "from-b"),
				);

				yield* a.sync();
				yield* b.sync();
				yield* a.pull();

				const rowsA = yield* Effect.tryPromise(
					() => a.all("SELECT id, body FROM messages") as Promise<unknown[]>,
				);
				const rowsB = yield* Effect.tryPromise(
					() => b.all("SELECT id, body FROM messages") as Promise<unknown[]>,
				);

				expect(rowsA).toEqual([{ id: 1, body: "from-b" }]);
				expect(rowsB).toEqual([{ id: 1, body: "from-b" }]);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});