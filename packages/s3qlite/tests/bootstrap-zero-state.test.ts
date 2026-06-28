import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

rustfs("bootstrap zero-state", (it) => {
	it.effect("first instance bootstraps empty state, second instance pulls it", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `test-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("bootstrap");
				const firstLocalKV = yield* makeMemoryKV();
				const secondLocalKV = yield* makeMemoryKV();

				// First instance: no local head, no remote head — bootstraps from scratch
				const first = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, firstLocalKV),
					),
					(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					first.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
				);
				yield* Effect.tryPromise(() =>
					first.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
				);
				yield* Effect.tryPromise(() =>
					first.run("INSERT INTO users (id, name) VALUES (?, ?)", "2", "bob"),
				);
				yield* first.push();

				const firstRows = yield* Effect.tryPromise(
					() => first.all("SELECT * FROM users ORDER BY id") as Promise<unknown[]>,
				);
				expect(firstRows).toEqual([
					{ id: "1", name: "alice" },
					{ id: "2", name: "bob" },
				]);

				// Second instance: no local head, remote head exists — pulls the bootstrapped state
				const second = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, secondLocalKV),
					),
					(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
				);

				const secondRows = yield* Effect.tryPromise(
					() => second.all("SELECT * FROM users ORDER BY id") as Promise<unknown[]>,
				);
				expect(secondRows).toEqual([
					{ id: "1", name: "alice" },
					{ id: "2", name: "bob" },
				]);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});