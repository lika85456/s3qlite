import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Option, Ref, Scope } from "effect";
import * as Reactivity from "effect/reactivity/Reactivity";
import { SqlClient } from "effect/sql/SqlClient";

import { makeMemoryKV } from "../src/kv/memoryKV";
import * as S3qliteClient from "../src/sql/client";
import { LocalKV, RemoteKV } from "../src/storage";
import type { ConnectionOptions } from "../src/types";

type KVService = (typeof LocalKV)["Service"];

// The SqlClient Connection API takes an optional row transform as a required positional argument.
const noTransform = Option.getOrUndefined(Option.none<never>());

const makeClient = (
	dbName: string,
	remote: KVService,
	local: KVService,
	options: Partial<ConnectionOptions> = {},
) =>
	S3qliteClient.make({ dbName, bucket: "test", ...options }).pipe(
		Effect.provideService(RemoteKV, remote),
		Effect.provideService(LocalKV, local),
		Effect.provide(Reactivity.layer),
	);

/** SqlClient + S3qlite layer over in-memory KV stores, like `S3qliteClient.layer` over S3. */
const memoryLayer = (dbName: string) =>
	S3qliteClient.layerFromKV({ dbName, bucket: "test" }).pipe(
		Layer.provide(Layer.effect(RemoteKV, makeMemoryKV())),
		Layer.provide(Layer.effect(LocalKV, makeMemoryKV())),
		Layer.provide(fileSystemLayer),
	);

describe("S3qliteClient (Effect SqlClient)", () => {
	it.effect("runs statements through the standard SqlClient", () =>
		Effect.gen(function* () {
			const sql = yield* SqlClient;
			yield* sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
			yield* sql`INSERT INTO ${sql("users")} ${sql.insert([
				{ id: 1, name: "alice" },
				{ id: 2, name: "bob" },
			])}`;

			const rows = yield* sql<{
				readonly id: number;
				readonly name: string;
			}>`SELECT id, name FROM users WHERE id IN ${sql.in([1, 2])} ORDER BY id`;
			expect(rows).toEqual([
				{ id: 1, name: "alice" },
				{ id: 2, name: "bob" },
			]);

			expect(yield* sql`SELECT id, name FROM users ORDER BY id`.values).toEqual([
				[1, "alice"],
				[2, "bob"],
			]);

			const inserted = yield* sql<{
				readonly id: number;
			}>`INSERT INTO users (id, name) VALUES (${3}, ${"carol"}) RETURNING id`;
			expect(inserted).toEqual([{ id: 3 }]);

			yield* sql`CREATE TABLE flags (id INTEGER PRIMARY KEY, enabled INTEGER NOT NULL)`;
			yield* sql`INSERT INTO flags ${sql.insert([
				{ id: 1, enabled: true },
				{ id: 2, enabled: false },
			])}`;
			expect(yield* sql`SELECT id, enabled FROM flags ORDER BY id`).toEqual([
				{ id: 1, enabled: 1 },
				{ id: 2, enabled: 0 },
			]);

			yield* sql`UPDATE users SET name = ${"bobby"} WHERE id = ${2}`;
			expect(yield* sql`SELECT name FROM users WHERE id = ${2}`).toEqual([{ name: "bobby" }]);
		}).pipe(Effect.provide(memoryLayer(`sql-${crypto.randomUUID()}`))),
	);

	it.effect("commits and rolls back transactions, including nested savepoints", () =>
		Effect.gen(function* () {
			const sql = yield* SqlClient;
			yield* sql`CREATE TABLE items (id INTEGER PRIMARY KEY)`;

			yield* sql.withTransaction(sql`INSERT INTO items VALUES (1)`);

			const failed = yield* Effect.exit(
				sql.withTransaction(
					sql`INSERT INTO items VALUES (2)`.pipe(Effect.andThen(Effect.fail("boom"))),
				),
			);
			expect(Exit.isFailure(failed)).toBe(true);

			yield* sql.withTransaction(
				Effect.gen(function* () {
					yield* sql`INSERT INTO items VALUES (3)`;
					const nested = yield* Effect.exit(
						sql.withTransaction(
							sql`INSERT INTO items VALUES (4)`.pipe(
								Effect.andThen(Effect.fail("inner")),
							),
						),
					);
					expect(Exit.isFailure(nested)).toBe(true);
				}),
			);

			expect(yield* sql`SELECT id FROM items ORDER BY id`).toEqual([{ id: 1 }, { id: 3 }]);
		}).pipe(Effect.provide(memoryLayer(`sql-${crypto.randomUUID()}`))),
	);

	it.effect("supports ORM-style usage (sqlite dialect, unsafe, reserve + BEGIN)", () =>
		Effect.gen(function* () {
			const sql = yield* SqlClient;

			expect(sql`SELECT * FROM t WHERE id = ${1} AND name = ${"a"}`.compile()).toEqual([
				"SELECT * FROM t WHERE id = ? AND name = ?",
				[1, "a"],
			]);
			expect(sql`SELECT ${sql("weird name")}`.compile()[0]).toBe('SELECT "weird name"');

			// Apps commonly run these pragmas through the client on startup.
			for (const pragma of [
				"PRAGMA journal_mode = WAL",
				"PRAGMA synchronous = NORMAL",
				"PRAGMA busy_timeout = 5000",
				"PRAGMA cache_size = -64000",
				"PRAGMA wal_checkpoint(PASSIVE)",
				"PRAGMA foreign_keys = ON",
			]) {
				yield* sql.unsafe(pragma).withoutTransform;
			}

			// The drizzle effect-sqlite session executes statements with `unsafe(...)`.
			yield* sql.unsafe("CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT)")
				.withoutTransform;
			yield* sql.unsafe("INSERT INTO session (id, title) VALUES (?, ?)", ["s1", "first"])
				.withoutTransform;
			expect(
				yield* sql.unsafe("SELECT id, title FROM session WHERE id = ?", ["s1"]).values,
			).toEqual([["s1", "first"]]);

			// ...and runs transactions on a reserved connection with plain BEGIN/COMMIT.
			const runOnReserved = (statement: string, failAfter: boolean) =>
				Effect.scoped(
					Effect.gen(function* () {
						const connection = yield* sql.reserve;
						yield* connection.executeUnprepared("begin deferred", [], noTransform);
						const work = Effect.gen(function* () {
							yield* sql.unsafe(statement).withoutTransform;
							if (failAfter) {
								return yield* Effect.fail("rollback me");
							}
						}).pipe(Effect.provideService(sql.transactionService, [connection, 0]));
						const exit = yield* Effect.exit(work);
						yield* connection.executeUnprepared(
							Exit.isSuccess(exit) ? "commit" : "rollback",
							[],
							noTransform,
						);
					}),
				);

			yield* runOnReserved("INSERT INTO session (id, title) VALUES ('s2', 'second')", false);
			yield* runOnReserved("INSERT INTO session (id, title) VALUES ('s3', 'third')", true);
			expect(yield* sql`SELECT id FROM session ORDER BY id`).toEqual([
				{ id: "s1" },
				{ id: "s2" },
			]);

			yield* sql.withTransaction(
				sql`UPDATE session SET title = ${"renamed"} WHERE id = ${"s1"}`,
			);
			expect(yield* sql`SELECT title FROM session WHERE id = ${"s1"}`).toEqual([
				{ title: "renamed" },
			]);
		}).pipe(Effect.provide(memoryLayer(`sql-${crypto.randomUUID()}`))),
	);

	it.effect("queues sync behind a running transaction", () =>
		Effect.gen(function* () {
			const sql = yield* SqlClient;
			const s3qlite = yield* S3qliteClient.S3qlite;
			const pulled = yield* Ref.make(false);
			yield* sql`CREATE TABLE items (id INTEGER PRIMARY KEY)`;

			const pullFiber = yield* sql.withTransaction(
				Effect.gen(function* () {
					const fiber = yield* s3qlite.pull.pipe(
						Effect.andThen(Ref.set(pulled, true)),
						Effect.forkChild({ startImmediately: true }),
					);
					for (let id = 0; id < 5; id++) {
						yield* sql`INSERT INTO items VALUES (${id})`;
					}
					expect(yield* Ref.get(pulled)).toBe(false);
					return fiber;
				}),
			);

			yield* Fiber.join(pullFiber);
			expect(yield* Ref.get(pulled)).toBe(true);
			expect(yield* sql`SELECT count(*) AS n FROM items`).toEqual([{ n: 5 }]);
		}).pipe(Effect.provide(memoryLayer(`sql-${crypto.randomUUID()}`))),
	);

	it.effect("syncs two clients through the S3qlite service", () =>
		Effect.gen(function* () {
			const name = `sql-${crypto.randomUUID()}`;
			const remote = yield* makeMemoryKV();
			const first = yield* makeClient(name, remote, yield* makeMemoryKV());
			const second = yield* makeClient(name, remote, yield* makeMemoryKV());

			yield* first`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)`;
			yield* first.withTransaction(first`INSERT INTO notes VALUES (1, 'from first')`);
			yield* first.s3qlite.push;

			yield* second.s3qlite.pull;
			yield* second`INSERT INTO notes VALUES (2, 'from second')`;
			yield* second.s3qlite.sync;
			yield* first.s3qlite.pull;

			const expected = [
				{ id: 1, body: "from first" },
				{ id: 2, body: "from second" },
			];
			expect(yield* first`SELECT * FROM notes ORDER BY id`).toEqual(expected);
			expect(yield* second`SELECT * FROM notes ORDER BY id`).toEqual(expected);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("pushes on change for statements and committed transactions", () =>
		Effect.gen(function* () {
			const name = `sql-${crypto.randomUUID()}`;
			const remote = yield* makeMemoryKV();
			const writer = yield* makeClient(name, remote, yield* makeMemoryKV(), {
				syncMode: { type: "push-on-change" },
			});
			const reader = yield* makeClient(name, remote, yield* makeMemoryKV());

			yield* writer`CREATE TABLE items (id INTEGER PRIMARY KEY)`;
			yield* writer`INSERT INTO items VALUES (1)`;
			yield* reader.s3qlite.pull;
			expect(yield* reader`SELECT id FROM items`).toEqual([{ id: 1 }]);

			yield* writer.withTransaction(
				Effect.all([
					writer`INSERT INTO items VALUES (2)`,
					writer`INSERT INTO items VALUES (3)`,
				]),
			);
			yield* Effect.exit(
				writer.withTransaction(
					writer`INSERT INTO items VALUES (4)`.pipe(Effect.andThen(Effect.fail("abort"))),
				),
			);
			yield* reader.s3qlite.pull;
			expect(yield* reader`SELECT id FROM items ORDER BY id`).toEqual([
				{ id: 1 },
				{ id: 2 },
				{ id: 3 },
			]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("pushes unpushed changes when the client scope closes", () =>
		Effect.gen(function* () {
			const name = `sql-${crypto.randomUUID()}`;
			const remote = yield* makeMemoryKV();
			const scope = yield* Scope.make();
			const writer = yield* makeClient(name, remote, yield* makeMemoryKV(), {
				pushOnClose: true,
			}).pipe(Scope.provide(scope));

			yield* writer`CREATE TABLE items (id INTEGER PRIMARY KEY)`;
			yield* writer`INSERT INTO items VALUES (42)`;
			yield* Scope.close(scope, Exit.void);

			const reader = yield* makeClient(name, remote, yield* makeMemoryKV());
			expect(yield* reader`SELECT id FROM items`).toEqual([{ id: 42 }]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});