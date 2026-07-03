import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

rustfs("composite-key-conflict", (it) => {
	it.effect("syncs rows with the same messageId across different userId values", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `composite-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("composite-key-conflict");
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
						"CREATE TABLE messages (userId INTEGER NOT NULL, messageId INTEGER NOT NULL, PRIMARY KEY (userId, messageId))",
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

				for (const [instance, instanceId] of [
					[a, 0],
					[b, 1],
				] as const) {
					yield* Effect.tryPromise(() =>
						instance.run(
							"INSERT INTO messages (messageId, userId) VALUES (?, ?)",
							1,
							instanceId,
						),
					);
				}

				yield* a.sync();
				yield* b.sync();
				yield* a.pull();

				const rowsA = yield* Effect.tryPromise(
					() =>
						a.all(
							"SELECT userId, messageId FROM messages ORDER BY userId, messageId",
						) as Promise<unknown[]>,
				);
				const rowsB = yield* Effect.tryPromise(
					() =>
						b.all(
							"SELECT userId, messageId FROM messages ORDER BY userId, messageId",
						) as Promise<unknown[]>,
				);

				expect(rowsA).toEqual([
					{ userId: 0, messageId: 1 },
					{ userId: 1, messageId: 1 },
				]);
				expect(rowsB).toEqual([
					{ userId: 0, messageId: 1 },
					{ userId: 1, messageId: 1 },
				]);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});