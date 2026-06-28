import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect, Option } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, batchKey, getJson, headKey } from "../src/storage";
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

rustfs("push pending batches only", (it) => {
	it.effect("pushes persisted pending batches without creating an extra batch", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `push-pending-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("push-pending");
				const localKVA = yield* makeMemoryKV();
				const localKVB = yield* makeMemoryKV();

				const a = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKVA),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					a.exec("CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL)"),
				);
				yield* a.push();

				const b = yield* connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKVB),
				);

				yield* b.pull();
				yield* Effect.tryPromise(() =>
					b.run("INSERT INTO items (id, value) VALUES (?, ?)", "b-1", "local-change"),
				);

				yield* Effect.tryPromise(() =>
					a.run("INSERT INTO items (id, value) VALUES (?, ?)", "a-1", "remote-change"),
				);
				yield* a.push();

				yield* b.pull();
				yield* Effect.tryPromise(() => b.close());

				const localBefore = yield* getStoredHead(dbName, localKVB);
				if (!localBefore.value.pendingBatches?.length) {
					throw new Error("expected pending batches");
				}

				const pendingBatchIds = localBefore.value.pendingBatches.map((batch) => batch.id);
				for (const batchId of pendingBatchIds) {
					expect(yield* localKVB.exists(batchKey(batchId))).toBe(true);
				}

				const remoteBefore = yield* getRemoteHead(dbName, remoteKV);

				const reopened = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKVB),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				yield* reopened.push();

				const localAfter = yield* getStoredHead(dbName, localKVB);
				const remoteAfter = yield* getRemoteHead(dbName, remoteKV);
				const appendedBatchIds = remoteAfter.value.batches
					.slice(remoteBefore.value.batches.length)
					.map((batch) => batch.id);

				expect(localAfter.value.pendingBatches ?? []).toHaveLength(0);
				expect(appendedBatchIds).toEqual(pendingBatchIds);
				expect(remoteAfter.value.batches).toHaveLength(
					remoteBefore.value.batches.length + pendingBatchIds.length,
				);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});