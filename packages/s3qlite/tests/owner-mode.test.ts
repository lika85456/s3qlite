import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Exit, Layer, Option, Schema, Scope } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import * as Idempotency from "../src/idempotency";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { HeadStore } from "../src/owner/headStore";
import { objectPrefix } from "../src/owner/owner";
import * as S3qliteClient from "../src/sql/client";
import { LocalKV, RemoteKV, getJson, headKey } from "../src/storage";
import { SyncModeError } from "../src/types";
import type { Head, OwnerSyncMode } from "../src/types";

type KVService = (typeof LocalKV)["Service"];

type Instance = {
	readonly context: Context.Context<
		S3qliteClient.S3qliteClient | SqlClient | S3qliteClient.S3qlite
	>;
	readonly scope: Scope.Closeable;
};

/** Opens an owner-mode `SqlClient` on `remote` with its own local directory (`local`). */
const openOwner = (
	dbName: string,
	remote: KVService,
	local: KVService,
	mode: Omit<OwnerSyncMode, "type"> = {},
) =>
	Effect.gen(function* () {
		const scope = yield* Scope.make();
		const context = yield* Layer.buildWithScope(
			S3qliteClient.layerFromKV({
				dbName,
				bucket: "test",
				syncMode: { type: "owner", ...mode },
			}).pipe(
				Layer.provide(Layer.succeed(RemoteKV, remote)),
				Layer.provide(Layer.succeed(LocalKV, local)),
			),
			scope,
		);
		return { context, scope } satisfies Instance;
	});

/** Small test kit; exported so the helpers stay small without lint noise. */
export const use = <A, E>(
	instance: Instance,
	effect: Effect.Effect<A, E, SqlClient | S3qliteClient.S3qlite>,
): Effect.Effect<A, E> => effect.pipe(Effect.provideContext(instance.context));

export const close = (instance: Instance) => Scope.close(instance.scope, Exit.void);

export const remoteHead = (remote: KVService, dbName: string) =>
	getJson<Head>(remote, headKey(dbName)).pipe(
		Effect.orDie,
		Effect.map((head) => Option.getOrThrow(head).value),
	);

const sql = Effect.service(SqlClient);

const memory = Effect.provide(fileSystemLayer);

// Live clock: the pusher sleeps (coalescing window, retry backoff).
describe("owner mode", () => {
	it.live("a durable write is in the remote head when it returns", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const a = yield* openOwner(dbName, remote, yield* makeMemoryKV());

			yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					yield* db`INSERT INTO notes (body) VALUES ('first')`;
				}),
			);
			const head = yield* remoteHead(remote, dbName);
			expect(head.generation).toBe(1);
			expect(head.batches).toHaveLength(2);
			expect(head.batches.every((batch) => batch.id.startsWith(objectPrefix(dbName)))).toBe(
				true,
			);

			// A second owner on another machine (own local directory) sees the write without any
			// close or explicit push on the first one.
			const b = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			const rows = yield* use(
				b,
				sql.pipe(Effect.flatMap((db) => db`SELECT body FROM notes`)),
			);
			expect(rows).toEqual([{ body: "first" }]);
			expect((yield* remoteHead(remote, dbName)).generation).toBe(2);
			yield* close(b);
			yield* close(a);
		}).pipe(Effect.scoped, memory),
	);

	it.live("the claim of a new owner fences the old one", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const a = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			yield* use(
				a,
				sql.pipe(
					Effect.flatMap(
						(db) => db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`,
					),
				),
			);

			const b = yield* openOwner(dbName, remote, yield* makeMemoryKV());

			// A has not noticed yet: its local commit happens, but the durable reply fails.
			const stale = yield* use(
				a,
				sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('stale')`)),
			).pipe(Effect.flip);
			expect(Option.map(S3qliteClient.ownerErrorOf(stale), (error) => error._tag)).toEqual(
				Option.some("OwnershipLostError"),
			);

			// From now on A rejects every statement, reads included.
			const after = yield* use(
				a,
				sql.pipe(Effect.flatMap((db) => db`SELECT * FROM notes`)),
			).pipe(Effect.flip);
			expect(Option.isSome(S3qliteClient.ownerErrorOf(after))).toBe(true);
			const status = yield* use(
				a,
				Effect.service(S3qliteClient.S3qlite).pipe(Effect.flatMap((s) => s.status)),
			);
			expect(Option.map(status, (s) => s.fenced)).toEqual(Option.some(true));

			// The stale write never became visible.
			yield* use(
				b,
				sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('b')`)),
			);
			const c = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			expect(
				yield* use(
					c,
					sql.pipe(Effect.flatMap((db) => db`SELECT body FROM notes ORDER BY id`)),
				),
			).toEqual([{ body: "b" }]);
			yield* Effect.all([close(a), close(b), close(c)]);
		}).pipe(Effect.scoped, memory),
	);

	it.live("fast replies return before the push; pushes coalesce; flush is a barrier", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const a = yield* openOwner(dbName, remote, yield* makeMemoryKV(), {
				reply: "fast",
				pushDelayMs: 60_000,
			});
			yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					for (let index = 0; index < 20; index += 1) {
						yield* db`INSERT INTO notes (body) VALUES (${`n${index}`})`;
					}
				}),
			);
			expect((yield* remoteHead(remote, dbName)).batches).toHaveLength(0);

			// A per-call durable override waits for the push (and pushes everything before it).
			yield* use(
				a,
				S3qliteClient.S3qlite.durable(
					sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('last')`)),
				),
			);
			expect((yield* remoteHead(remote, dbName)).batches).toHaveLength(1);

			yield* use(a, sql.pipe(Effect.flatMap((db) => db`DELETE FROM notes WHERE id = 1`)));
			yield* use(
				a,
				Effect.service(S3qliteClient.S3qlite).pipe(Effect.flatMap((s) => s.flush)),
			);
			expect((yield* remoteHead(remote, dbName)).batches).toHaveLength(2);
			yield* close(a);
		}).pipe(Effect.scoped, memory),
	);

	it.live("durable transactions wait once, after COMMIT", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const a = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					yield* db.withTransaction(
						Effect.gen(function* () {
							yield* db`INSERT INTO notes (body) VALUES ('a')`;
							yield* db.withTransaction(db`INSERT INTO notes (body) VALUES ('b')`);
						}),
					);
				}),
			);
			expect((yield* remoteHead(remote, dbName)).batches).toHaveLength(2);
			yield* close(a);
		}).pipe(Effect.scoped, memory),
	);

	it.live("reopening on the same local directory skips the download", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const local = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const status = (instance: Instance) =>
				use(
					instance,
					Effect.service(S3qliteClient.S3qlite).pipe(
						Effect.flatMap((s) => s.status),
						Effect.map(Option.getOrThrow),
					),
				);

			const first = yield* openOwner(dbName, remote, local);
			expect((yield* status(first)).open.created).toBe(true);
			yield* use(
				first,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					yield* db`INSERT INTO notes (body) VALUES ('kept')`;
				}),
			);
			yield* close(first);

			const second = yield* openOwner(dbName, remote, local);
			expect((yield* status(second)).open).toMatchObject({
				downloaded: false,
				created: false,
			});
			expect(
				yield* use(second, sql.pipe(Effect.flatMap((db) => db`SELECT body FROM notes`))),
			).toEqual([{ body: "kept" }]);
			yield* close(second);

			// Another owner wrote in between: the local copy is stale and is downloaded again.
			const other = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			yield* use(
				other,
				sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('other')`)),
			);
			yield* close(other);
			const third = yield* openOwner(dbName, remote, local);
			expect((yield* status(third)).open.downloaded).toBe(true);
			expect(
				yield* use(
					third,
					sql.pipe(Effect.flatMap((db) => db`SELECT body FROM notes ORDER BY id`)),
				),
			).toEqual([{ body: "kept" }, { body: "other" }]);
			yield* close(third);
		}).pipe(Effect.scoped, memory),
	);

	it.live("prunes turso_cdc after every push and reports exact run() results", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const a = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			const client = Context.get(a.context, S3qliteClient.S3qliteClient);
			yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					for (let index = 0; index < 200; index += 1) {
						yield* db`INSERT INTO notes (body) VALUES (${`n${index}`})`;
					}
					const [cdc] = yield* db<{
						readonly n: number;
					}>`SELECT count(*) AS n FROM turso_cdc`;
					// Only the newest pushed row stays (it keeps change ids growing).
					expect(cdc.n).toBeLessThanOrEqual(2);
					const raw = (yield* db`UPDATE notes SET body = 'x' WHERE id <= 3`.raw) as {
						readonly changes: number;
					};
					expect(raw.changes).toBe(3);
					const inserted = (yield* db`INSERT INTO notes (body) VALUES ('rowid')`.raw) as {
						readonly lastInsertRowid: number;
					};
					expect(inserted.lastInsertRowid).toBe(201);
				}),
			);
			expect(client.config.dbName).toBe(dbName);
			yield* close(a);
		}).pipe(Effect.scoped, memory),
	);

	it.live("snapshots compact the head, restore stays correct and GC removes old objects", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const a = yield* openOwner(dbName, remote, yield* makeMemoryKV(), {
				snapshot: { everyBatches: 5 },
				gc: { minAgeMs: 0 },
			});
			yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					for (let index = 0; index < 23; index += 1) {
						yield* db`INSERT INTO notes (body) VALUES (${`n${index}`})`;
					}
				}),
			);
			// Snapshots and GC run in the pusher after the reply; a snapshot round waits for them.
			yield* use(
				a,
				Effect.service(S3qliteClient.S3qlite).pipe(Effect.flatMap((s) => s.snapshot)),
			);
			// The manual snapshot replaced the automatic one, so GC finds the old one.
			const garbage = yield* use(
				a,
				Effect.service(S3qliteClient.S3qlite).pipe(Effect.flatMap((s) => s.gc)),
			);
			expect(garbage.length).toBeGreaterThan(0);

			const head = yield* remoteHead(remote, dbName);
			expect(head.snapshots).toHaveLength(1);
			expect(head.batches.length).toBeLessThanOrEqual(1);
			const objects = yield* Effect.promise(() => Promise.resolve()).pipe(
				Effect.andThen(remote.list?.(objectPrefix(dbName)) ?? Effect.succeed([])),
			);
			// Only what the head references survives GC.
			expect(objects.map((object) => object.key).sort()).toEqual(
				[
					...head.snapshots.map((s) => `${s.id}.snapshot`),
					...head.batches.map((b) => `${b.id}.batch`),
				].sort(),
			);

			const b = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			const rows = yield* use(
				b,
				sql.pipe(Effect.flatMap((db) => db`SELECT count(*) AS n FROM notes`)),
			);
			expect(rows).toEqual([{ n: 23 }]);
			// The restored copy does not push the snapshot's CDC rows again.
			yield* use(
				b,
				sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('after')`)),
			);
			const c = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			expect(
				yield* use(
					c,
					sql.pipe(Effect.flatMap((db) => db`SELECT count(*) AS n FROM notes`)),
				),
			).toEqual([{ n: 24 }]);
			yield* Effect.all([close(a), close(b), close(c)]);
		}).pipe(Effect.scoped, memory),
	);

	it.live("GC keeps objects referenced by a fork", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			const forkName = `${dbName}-fork`;
			const a = yield* openOwner(dbName, remote, yield* makeMemoryKV(), {
				snapshot: false,
				gc: { minAgeMs: 0 },
			});
			const s3qlite = Effect.service(S3qliteClient.S3qlite);
			yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					yield* db`INSERT INTO notes (body) VALUES ('shared')`;
					yield* (yield* s3qlite).fork(forkName);
					yield* db`INSERT INTO notes (body) VALUES ('source only')`;
					yield* (yield* s3qlite).snapshot;
					yield* (yield* s3qlite).gc;
				}),
			);
			const fork = yield* openOwner(forkName, remote, yield* makeMemoryKV());
			expect(
				yield* use(fork, sql.pipe(Effect.flatMap((db) => db`SELECT body FROM notes`))),
			).toEqual([{ body: "shared" }]);
			yield* Effect.all([close(a), close(fork)]);
		}).pipe(Effect.scoped, memory),
	);

	it.live("a lost compare-and-set response is adopted instead of fencing", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			let failNextCas = false;
			const flaky: KVService = {
				...remote,
				cas: (key, value, etag) =>
					remote.cas(key, value, etag).pipe(
						Effect.tap(() => {
							if (failNextCas) {
								failNextCas = false;
								return Effect.die(new Error("connection reset after the write"));
							}
							return Effect.void;
						}),
					),
			};
			const a = yield* openOwner(dbName, flaky, yield* makeMemoryKV());
			yield* use(
				a,
				sql.pipe(
					Effect.flatMap(
						(db) => db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`,
					),
				),
			);
			failNextCas = true;
			const unknown = yield* use(
				a,
				sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('maybe')`)),
			).pipe(Effect.flip);
			expect(Option.map(S3qliteClient.ownerErrorOf(unknown), (e) => e._tag)).toEqual(
				Option.some("PushFailedError"),
			);
			// The next write works and the uncertain one is published exactly once.
			yield* use(
				a,
				sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('next')`)),
			);
			const b = yield* openOwner(dbName, remote, yield* makeMemoryKV());
			expect(
				yield* use(
					b,
					sql.pipe(Effect.flatMap((db) => db`SELECT body FROM notes ORDER BY id`)),
				),
			).toEqual([{ body: "maybe" }, { body: "next" }]);
			yield* Effect.all([close(a), close(b)]);
		}).pipe(Effect.scoped, memory),
	);

	it.live("pull and sync are rejected in owner mode", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const a = yield* openOwner(
				`owner-${crypto.randomUUID()}`,
				remote,
				yield* makeMemoryKV(),
			);
			const s3qlite = Context.get(a.context, S3qliteClient.S3qlite);
			expect(yield* Effect.flip(s3qlite.pull)).toBeInstanceOf(SyncModeError);
			expect(yield* Effect.flip(s3qlite.sync)).toBeInstanceOf(SyncModeError);
			yield* close(a);
		}).pipe(Effect.scoped, memory),
	);

	it.live("Idempotency.once applies a write id once and replays its result", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const a = yield* openOwner(
				`owner-${crypto.randomUUID()}`,
				remote,
				yield* makeMemoryKV(),
			);
			const append = (writeId: string) =>
				Idempotency.once(
					{ writeId, success: Schema.Struct({ count: Schema.Number }) },
					Effect.gen(function* () {
						const db = yield* sql;
						yield* db`INSERT INTO entries (text) VALUES (${writeId})`;
						const [row] = yield* db<{
							readonly n: number;
						}>`SELECT count(*) AS n FROM entries`;
						return { count: row.n };
					}),
				);
			const results = yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE entries (id INTEGER PRIMARY KEY, text TEXT NOT NULL)`;
					return [
						yield* append("w1"),
						yield* append("w2"),
						yield* append("w1"),
						yield* Idempotency.isApplied("w2"),
						yield* Idempotency.isApplied("w3"),
					];
				}),
			);
			expect(results).toEqual([{ count: 1 }, { count: 2 }, { count: 1 }, true, false]);
			yield* close(a);
		}).pipe(Effect.scoped, memory),
	);

	it.live("works with an external head store", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const store = yield* makeMemoryKV();
			const dbName = `owner-${crypto.randomUUID()}`;
			// A HeadStore over a separate KV stands in for Postgres here; the cluster suite uses Postgres.
			const external = { ...HeadStore.remote(store), external: true };
			const withExternal = Effect.provideService(HeadStore, external);
			const a = yield* withExternal(openOwner(dbName, remote, yield* makeMemoryKV()));
			yield* use(
				a,
				Effect.gen(function* () {
					const db = yield* sql;
					yield* db`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
					yield* db`INSERT INTO notes (body) VALUES ('x')`;
				}),
			);
			// The bucket gets a mirror of the authoritative head.
			expect((yield* remoteHead(remote, dbName)).batches).toHaveLength(2);
			const b = yield* withExternal(openOwner(dbName, remote, yield* makeMemoryKV()));
			const stale = yield* use(
				a,
				sql.pipe(Effect.flatMap((db) => db`INSERT INTO notes (body) VALUES ('stale')`)),
			).pipe(Effect.flip);
			expect(Option.isSome(S3qliteClient.ownerErrorOf(stale))).toBe(true);
			expect(
				yield* use(b, sql.pipe(Effect.flatMap((db) => db`SELECT body FROM notes`))),
			).toEqual([{ body: "x" }]);
			yield* Effect.all([close(a), close(b)]);
		}).pipe(Effect.scoped, memory),
	);
});