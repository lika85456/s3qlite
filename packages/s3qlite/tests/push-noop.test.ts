import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect, Option } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, getJson, headKey } from "../src/storage";
import type { Head, StoredHead } from "../src/types";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

const getStoredHead = (dbName: string, kv: LocalKV["Type"]) =>
	getJson<StoredHead>(kv, headKey(dbName)).pipe(
		Effect.flatMap(
			Option.match({
				onNone: () => Effect.die("missing stored head"),
				onSome: (head) => Effect.succeed(head),
			}),
		),
	);

const getRemoteHead = (dbName: string, kv: RemoteKV["Type"]) =>
	getJson<Head>(kv, headKey(dbName)).pipe(
		Effect.flatMap(
			Option.match({
				onNone: () => Effect.die("missing remote head"),
				onSome: (head) => Effect.succeed(head),
			}),
		),
	);

rustfs("push no-op", (it) => {
	it.effect("does nothing when there are no pending batches and no new local changes", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `push-noop-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("push-noop");
				const localKV = yield* makeMemoryKV();

				const db = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					db.exec("CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL)"),
				);
				yield* Effect.tryPromise(() =>
					db.run("INSERT INTO items (id, value) VALUES (?, ?)", "1", "seed"),
				);
				yield* db.push();

				const localBefore = yield* getStoredHead(dbName, localKV);
				const remoteBefore = yield* getRemoteHead(dbName, remoteKV);

				yield* db.push();

				const localAfter = yield* getStoredHead(dbName, localKV);
				const remoteAfter = yield* getRemoteHead(dbName, remoteKV);

				expect(localAfter).toEqual(localBefore);
				expect(remoteAfter).toEqual(remoteBefore);
				expect(remoteAfter.value.batches).toHaveLength(1);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});