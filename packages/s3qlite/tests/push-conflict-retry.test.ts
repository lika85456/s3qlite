import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { ConflictError } from "../src/kv/kv";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, headKey } from "../src/storage";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

rustfs("push-conflict-retry", (it) => {
	it.effect("recovers from push conflict via sync", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `test-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("push-conflict");
				const localKVA = yield* makeMemoryKV();
				const localKVB = yield* makeMemoryKV();

				// Seed: create schema and push to establish remote head
				const seeder = yield* connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKVA),
				);
				yield* Effect.tryPromise(() =>
					seeder.exec("CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL)"),
				);
				yield* seeder.push();
				yield* Effect.tryPromise(() => seeder.close());

				// Open two instances from the same remote head
				const a = yield* connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKVA),
				);
				const b = yield* connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKVB),
				);

				// Both write non-colliding rows
				yield* Effect.tryPromise(() =>
					a.run("INSERT INTO items (id, value) VALUES ('a', 'from-a')"),
				);
				yield* Effect.tryPromise(() =>
					b.run("INSERT INTO items (id, value) VALUES ('b', 'from-b')"),
				);

				// A pushes successfully
				yield* a.push();

				// CAS rejects B's stale head etag, so push surfaces a ConflictError.
				{
					const conflict = yield* Effect.flip(b.push());
					expect(conflict).toBeInstanceOf(ConflictError);
					expect(conflict._tag).toBe("ConflictError");
					expect(conflict.key).toBe(headKey(dbName));
				}

				// B syncs — recovers via pull+push
				yield* b.sync();

				// A pulls to converge
				yield* a.pull();

				// Both see all rows
				const rowsA = yield* Effect.tryPromise(
					() => a.all("SELECT id, value FROM items ORDER BY id") as Promise<unknown[]>,
				);
				const rowsB = yield* Effect.tryPromise(
					() => b.all("SELECT id, value FROM items ORDER BY id") as Promise<unknown[]>,
				);

				expect(rowsA).toEqual([
					{ id: "a", value: "from-a" },
					{ id: "b", value: "from-b" },
				]);
				expect(rowsB).toEqual([
					{ id: "a", value: "from-a" },
					{ id: "b", value: "from-b" },
				]);

				// Cleanup
				yield* Effect.tryPromise(() => a.close());
				yield* Effect.tryPromise(() => b.close());
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});