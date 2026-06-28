import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import { Effect, Option } from "effect";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { connect } from "../src/connection";
import type { KV } from "../src/kv/kv";
import { ConflictError } from "../src/kv/kv";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, batchKey, getJson, headKey } from "../src/storage";
import type { StoredHead } from "../src/types";

// oxlint-disable-next-line local-rules/function-minimum-length
const schemaSql = "CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL)";

// oxlint-disable-next-line local-rules/function-minimum-length
const readRows = async (db: Database): Promise<unknown[]> => {
	const rows = await db.all("SELECT id, value FROM items ORDER BY id");
	return rows as unknown[];
};

const getStoredHead = (kv: KV, dbName: string) =>
	getJson<StoredHead>(kv, headKey(dbName)).pipe(
		Effect.flatMap(
			Option.match({
				onNone: () => Effect.die("no stored head"),
				onSome: (h) => Effect.succeed(h.value),
			}),
		),
	);

// ponytail: fail-first-cas wrapper, single failure injection
const makeFaultyRemoteKV = (inner: KV): KV => {
	let failNext = true;
	return {
		get: inner.get,
		getIfChanged: inner.getIfChanged,
		exists: inner.exists,
		set: inner.set,
		delete: inner.delete,
		readStream: inner.readStream,
		writeStream: inner.writeStream,
		cas: (key, value, etag) => {
			if (failNext) {
				failNext = false;
				return Effect.fail(new ConflictError({ key }));
			}
			return inner.cas(key, value, etag);
		},
	};
};

describe("pending batch retry", () => {
	it("retries pending batches after failed push", async () => {
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const dir = yield* Effect.acquireRelease(
						Effect.promise(() => mkdtemp(join(tmpdir(), "s3qlite-retry-"))),
						(d) => Effect.promise(() => rm(d, { recursive: true, force: true })),
					);
					const control = yield* Effect.acquireRelease(
						Effect.promise(() => connectTurso(join(dir, "control.db"))),
						(db) => Effect.promise(() => db.close()),
					);

					const dbName = `retry-${crypto.randomUUID().slice(0, 8)}`;
					const remoteKV = yield* makeMemoryKV();
					const localKVA = yield* makeMemoryKV();
					const localKVB = yield* makeMemoryKV();

					// A: create schema + data, push
					const a = yield* connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKVA),
					);
					yield* Effect.promise(() => a.exec(schemaSql));
					yield* Effect.promise(() => control.exec(schemaSql));
					yield* a.push();

					// B: pull, create local change
					const b = yield* connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKVB),
					);
					yield* b.pull();
					yield* Effect.promise(() =>
						b.run("INSERT INTO items (id, value) VALUES ('b-1', 'v1')"),
					);
					yield* Effect.promise(() =>
						control.run("INSERT INTO items (id, value) VALUES ('b-1', 'v1')"),
					);

					// A: create change, push
					yield* Effect.promise(() =>
						a.run("INSERT INTO items (id, value) VALUES ('a-1', 'v1')"),
					);
					yield* Effect.promise(() =>
						control.run("INSERT INTO items (id, value) VALUES ('a-1', 'v1')"),
					);
					yield* a.push();

					// B pulls → pending batches accumulate
					yield* b.pull();

					// Verify pending batches exist + batch blob stored locally
					{
						const head = yield* getStoredHead(localKVB, dbName);
						expect(head.pendingBatches).toBeDefined();
						if (!head.pendingBatches) {
							throw new Error("pendingBatches missing");
						}
						expect(head.pendingBatches.length).toBe(1);
						expect(yield* localKVB.exists(batchKey(head.pendingBatches[0].id))).toBe(
							true,
						);
					}

					// Close B's DB connection before re-opening with same localKV
					yield* Effect.promise(() => b.close());

					// Connect B' with faulty remote KV. The injected CAS failure should surface
					// as ConflictError from push, not just a generic left.
					const bFaulty = yield* connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, makeFaultyRemoteKV(remoteKV)),
						Effect.provideService(LocalKV, localKVB),
					);
					const conflict = yield* Effect.flip(bFaulty.push());
					expect(conflict).toBeInstanceOf(ConflictError);
					expect(conflict._tag).toBe("ConflictError");

					// Pending batches survive the failed push
					{
						const head = yield* getStoredHead(localKVB, dbName);
						expect(head.pendingBatches).toBeDefined();
						if (!head.pendingBatches) {
							throw new Error("pendingBatches missing");
						}
						expect(head.pendingBatches.length).toBeGreaterThan(0);
						const exists = yield* localKVB.exists(batchKey(head.pendingBatches[0].id));
						expect(exists).toBe(true);
					}

					yield* Effect.promise(() => bFaulty.close());

					// Connect B'' with healthy remote KV → push drains pending batches
					const bHealthy = yield* connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKVB),
					);
					yield* bHealthy.push();

					// Pending batches drained
					{
						const head = yield* getStoredHead(localKVB, dbName);
						expect(head.pendingBatches?.length ?? 0).toBe(0);
					}

					// A pulls to verify remote/local convergence
					yield* a.pull();
					{
						const act = yield* Effect.promise(() => readRows(a as unknown as Database));
						const exp = yield* Effect.promise(() => readRows(control));
						expect(act).toEqual(exp);
					}

					yield* Effect.promise(() => bHealthy.close());
				}),
			).pipe(Effect.provide(fileSystemLayer)),
		);
	});
});