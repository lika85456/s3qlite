import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

describe("s3qlite schema mutation sync", () => {
	it.scopedLive("CREATE TABLE, ALTER TABLE ADD COLUMN, DROP TABLE across two instances", () =>
		Effect.gen(function* () {
			const dbName = `schema-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const localA = yield* makeMemoryKV();
			const localB = yield* makeMemoryKV();

			// --- Instance A: create schema, insert data, push ---
			const a = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localA),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* Effect.tryPromise(() =>
				a.exec("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
			);
			yield* Effect.tryPromise(() =>
				a.run("INSERT INTO items (id, name) VALUES (?, ?)", "1", "first"),
			);
			yield* a.push();

			// --- Instance B: pull, verify schema + data, ALTER TABLE, insert, push ---
			const b = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localB),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* b.pull();

			// Verify B sees the table and data
			const bRowsBefore = yield* Effect.tryPromise(
				() => b.all("SELECT id, name FROM items ORDER BY id") as Promise<unknown[]>,
			);
			expect(bRowsBefore).toEqual([{ id: "1", name: "first" }]);

			// Alter table: add column
			yield* Effect.tryPromise(() =>
				b.exec("ALTER TABLE items ADD COLUMN description TEXT DEFAULT 'default'"),
			);
			yield* Effect.tryPromise(() =>
				b.run(
					"INSERT INTO items (id, name, description) VALUES (?, ?, ?)",
					"2",
					"second",
					"two",
				),
			);
			yield* b.push();

			// --- Instance A: pull, verify it has the new column and data ---
			yield* a.pull();

			const aRows = yield* Effect.tryPromise(
				() =>
					a.all("SELECT id, name, description FROM items ORDER BY id") as Promise<
						unknown[]
					>,
			);
			expect(aRows).toEqual([
				{ id: "1", name: "first", description: "default" },
				{ id: "2", name: "second", description: "two" },
			]);

			// --- Instance A: drop table, push ---
			yield* Effect.tryPromise(() => a.exec("DROP TABLE items"));
			yield* a.push();

			// --- Instance B: pull, verify table is gone ---
			yield* b.pull();

			const bSchemaAfter = yield* Effect.tryPromise(
				() =>
					b.all(
						"SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'items'",
					) as Promise<unknown[]>,
			);
			expect(bSchemaAfter).toHaveLength(0);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.scopedLive("CREATE INDEX and DROP INDEX across two instances", () =>
		Effect.gen(function* () {
			const dbName = `idx-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const localA = yield* makeMemoryKV();
			const localB = yield* makeMemoryKV();

			const a = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localA),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* Effect.tryPromise(() =>
				a.exec(
					"CREATE TABLE products (id TEXT PRIMARY KEY, sku TEXT NOT NULL, price REAL)",
				),
			);
			yield* Effect.tryPromise(() =>
				a.run("INSERT INTO products (id, sku, price) VALUES (?, ?, ?)", "p1", "A100", 9.99),
			);
			yield* Effect.tryPromise(() =>
				a.exec("CREATE UNIQUE INDEX idx_products_sku ON products(sku)"),
			);
			yield* a.push();

			const b = yield* Effect.acquireRelease(
				connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localB),
				),
				(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
			);

			yield* b.pull();

			// Verify index exists by checking unique constraint works
			const insertError = yield* Effect.either(
				Effect.tryPromise(() =>
					b.run(
						"INSERT INTO products (id, sku, price) VALUES (?, ?, ?)",
						"p2",
						"A100",
						19.99,
					),
				),
			);
			expect(insertError._tag).toBe("Left");

			// Drop index, push
			yield* Effect.tryPromise(() => b.exec("DROP INDEX idx_products_sku"));
			yield* Effect.tryPromise(() =>
				b.run(
					"INSERT INTO products (id, sku, price) VALUES (?, ?, ?)",
					"p2",
					"A100",
					19.99,
				),
			);
			yield* b.push();

			// A pulls and verifies duplicate SKU is now allowed
			yield* a.pull();
			yield* Effect.tryPromise(() =>
				a.run(
					"INSERT INTO products (id, sku, price) VALUES (?, ?, ?)",
					"p3",
					"A100",
					29.99,
				),
			);

			const aRows = yield* Effect.tryPromise(
				() =>
					a.all(
						"SELECT id, sku, price FROM products WHERE sku = 'A100' ORDER BY id",
					) as Promise<unknown[]>,
			);
			expect(aRows).toHaveLength(3);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});