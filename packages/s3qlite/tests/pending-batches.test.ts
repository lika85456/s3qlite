import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import { Effect, Option } from "effect";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, batchKey, getJson, headKey } from "../src/storage";
import type { StoredHead } from "../src/types";

const schemaSql =
	"CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL)";

// oxlint-disable-next-line local-rules/function-minimum-length
const readRows = async (db: Database): Promise<unknown[]> => {
	const rows = await db.all("SELECT id, value, version FROM items ORDER BY id");
	return rows as unknown[];
};

describe("pending batches", () => {
	it("accumulates across pulls and drains on push", async () => {
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const dir = yield* Effect.acquireRelease(
						Effect.promise(() => mkdtemp(join(tmpdir(), "s3qlite-pending-"))),
						(d) => Effect.promise(() => rm(d, { recursive: true, force: true })),
					);
					const control = yield* Effect.acquireRelease(
						Effect.promise(() => connectTurso(join(dir, "control.db"))),
						(db) => Effect.promise(() => db.close()),
					);

					const dbName = "pending-test";
					const remoteKV = yield* makeMemoryKV();
					const localKVA = yield* makeMemoryKV();
					const localKVB = yield* makeMemoryKV();

					const a = yield* connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKVA),
					);
					yield* Effect.promise(() => a.exec(schemaSql));
					yield* Effect.promise(() => control.exec(schemaSql));
					yield* a.push();

					const b = yield* connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKVB),
					);

					// Pull 1: a pushes a change, b pulls while having a local change
					yield* Effect.promise(() =>
						a.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('a-1', 'v1', 1)",
						),
					);
					yield* Effect.promise(() =>
						control.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('a-1', 'v1', 1)",
						),
					);
					yield* a.pull();
					yield* a.push();

					yield* Effect.promise(() =>
						b.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('b-1', 'v1', 1)",
						),
					);
					yield* Effect.promise(() =>
						control.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('b-1', 'v1', 1)",
						),
					);
					yield* b.pull();

					{
						const head = yield* getJson<StoredHead>(localKVB, headKey(dbName)).pipe(
							Effect.flatMap(
								Option.match({
									onNone: () => Effect.die("no head"),
									onSome: (h) => Effect.succeed(h.value),
								}),
							),
						);
						expect(head.pendingBatches).toBeDefined();
						expect(head.pendingBatches?.length).toBe(1);
						if (!head.pendingBatches) {
							throw new Error("pendingBatches missing");
						}
						expect(yield* localKVB.exists(batchKey(head.pendingBatches[0].id))).toBe(
							true,
						);
					}

					// Pull 2: a pushes another change, b pulls while having another local change
					yield* Effect.promise(() =>
						a.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('a-2', 'v2', 2)",
						),
					);
					yield* Effect.promise(() =>
						control.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('a-2', 'v2', 2)",
						),
					);
					yield* a.pull();
					yield* a.push();

					yield* Effect.promise(() =>
						b.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('b-2', 'v2', 2)",
						),
					);
					yield* Effect.promise(() =>
						control.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('b-2', 'v2', 2)",
						),
					);
					yield* b.pull();

					{
						const head = yield* getJson<StoredHead>(localKVB, headKey(dbName)).pipe(
							Effect.flatMap(
								Option.match({
									onNone: () => Effect.die("no head"),
									onSome: (h) => Effect.succeed(h.value),
								}),
							),
						);
						expect(head.pendingBatches).toBeDefined();
						if (!head.pendingBatches) {
							throw new Error("pendingBatches missing");
						}
						expect(head.pendingBatches.length).toBe(2);
						expect(yield* localKVB.exists(batchKey(head.pendingBatches[1].id))).toBe(
							true,
						);
					}

					// Live change (no pull), then push — should drain both pending batches + live-extract
					yield* Effect.promise(() =>
						b.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('b-3', 'v3', 3)",
						),
					);
					yield* Effect.promise(() =>
						control.run(
							"INSERT OR REPLACE INTO items (id, value, version) VALUES ('b-3', 'v3', 3)",
						),
					);
					yield* b.push();

					{
						const act = yield* Effect.promise(() => readRows(b));
						const exp = yield* Effect.promise(() => readRows(control));
						expect(act).toEqual(exp);
					}
				}),
			).pipe(Effect.provide(fileSystemLayer)),
		);
	});
});