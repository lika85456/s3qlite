import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Option } from "effect";

import { serializeCDC } from "../src/cdc/protobuf";
import { connect } from "../src/connection";
import type { KV } from "../src/kv/kv";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, batchKey, encodeJson, headKey } from "../src/storage";
import type { Head } from "../src/types";

const textDecoder = new TextDecoder();

/**
 * Wraps a base KV so that on the second CAS to the head key, a competing
 * empty batch is pushed to the remote first, advancing the head and
 * causing the original CAS to fail with a stale etag.
 *
 * The first head CAS passes through untouched (initial push).
 *
 * ponytail: mutable counter in closure; replace with atomic Ref if
 * concurrent CAS interception is ever needed.
 */
const makeStaleHeadRemoteKV = (baseKV: KV, dbName: string): KV => {
	let casCount = 0;
	const emptyBatchId = crypto.randomUUID();
	const emptyBatchBytes = serializeCDC([]);

	return {
		...baseKV,
		cas: (key: string, value: Uint8Array, etag: string) => {
			if (key === headKey(dbName)) {
				casCount++;
				if (casCount === 2) {
					// Fire on second head CAS (first push succeeds, second push triggers staleness)
					return Effect.gen(function* () {
						yield* baseKV.set(batchKey(emptyBatchId), emptyBatchBytes);
						const currentOpt = yield* baseKV.get(headKey(dbName));
						if (Option.isSome(currentOpt)) {
							const head = JSON.parse(
								textDecoder.decode(currentOpt.value.value),
							) as Head;
							head.batches = [...head.batches, { id: emptyBatchId }];
							yield* baseKV.set(headKey(dbName), encodeJson(head));
						}
						return yield* baseKV.cas(key, value, etag);
					});
				}
			}
			return baseKV.cas(key, value, etag);
		},
	};
};

describe("stale-head sync", () => {
	it.scopedLive("recovers from stale head during sync via retry", () =>
		Effect.gen(function* () {
			const dbName = `test-${crypto.randomUUID().slice(0, 8)}`;
			const baseRemoteKV = yield* makeMemoryKV();
			const remoteKV = makeStaleHeadRemoteKV(baseRemoteKV, dbName);
			const localKV = yield* makeMemoryKV();

			const db = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKV),
				),
				(d) => Effect.tryPromise(() => d.close()).pipe(Effect.orDie),
			);

			yield* Effect.tryPromise(() =>
				db.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
			);
			yield* Effect.tryPromise(() =>
				db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
			);
			yield* db.push();

			// Local-only change, not yet pushed — will trigger a stale head on push
			yield* Effect.tryPromise(() =>
				db.run("INSERT INTO users (id, name) VALUES (?, ?)", "2", "bob"),
			);

			// `sync()` retries the whole pull+push sequence. The decorator advances the
			// remote head on the first sync push attempt, forcing a stale-head retry.
			yield* db.sync();

			const users = yield* Effect.tryPromise(
				() => db.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
			);
			expect(users).toEqual([
				{ id: "1", name: "alice" },
				{ id: "2", name: "bob" },
			]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});