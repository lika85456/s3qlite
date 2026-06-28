import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "@effect/vitest";
import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";

import { replayCDC } from "./apply";
import { extractCDC } from "./extract";
import type { DatabaseSnapshot } from "./testUtils";
import { snapshotDatabase } from "./testUtils";
import type { CDCRow } from "./types";

const replayAndSnapshot = (
	sourceDatabase: Database,
	targetDatabase: Database,
): Effect.Effect<readonly [readonly CDCRow[], DatabaseSnapshot, DatabaseSnapshot], Error> =>
	Effect.gen(function* () {
		const changes = yield* extractCDC(sourceDatabase, 0);
		yield* replayCDC(targetDatabase, changes);
		const sourceSnapshot = yield* snapshotDatabase(sourceDatabase);
		const targetSnapshot = yield* snapshotDatabase(targetDatabase);
		return [changes, sourceSnapshot, targetSnapshot] as const;
	});

describe("cdc", () => {
	let sourceDb: Database;
	let targetDb: Database;
	let baseDir: string;
	let sourcePath: string;
	let targetPath: string;

	beforeAll(async () => {
		baseDir = await mkdtemp("/dev/shm/s3qlite-cdc-");
	});

	afterAll(async () => {
		await rm(baseDir, { recursive: true, force: true });
	});

	beforeEach(async () => {
		[sourcePath, targetPath] = [
			join(baseDir, `${crypto.randomUUID()}-source.sqlite`),
			join(baseDir, `${crypto.randomUUID()}-target.sqlite`),
		];
		sourceDb = await connectTurso(sourcePath);
		targetDb = await connectTurso(targetPath);
		await sourceDb.exec("PRAGMA capture_data_changes_conn('full')");
	});

	afterEach(async () => {
		await Promise.all([sourceDb.close(), targetDb.close()]);
	});

	it.effect("replays schema creation, inserts, updates and deletes", () =>
		Effect.gen(function* () {
			yield* Effect.forEach(
				[
					"CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT NOT NULL, age INTEGER NOT NULL, score REAL, avatar BLOB, bio TEXT)",
					"CREATE UNIQUE INDEX users_email_idx ON users(email)",
					"INSERT INTO users (id, email, age, score, avatar, bio) VALUES ('u1', 'alice@example.com', 30, 10.5, x'010203', 'alpha')",
					"INSERT INTO users (id, email, age, score, avatar, bio) VALUES ('u2', 'bob@example.com', 41, 8.25, x'0A0B', NULL)",
					"UPDATE users SET age = 31, score = 11.75, bio = 'alpha-2' WHERE id = 'u1'",
					"DELETE FROM users WHERE id = 'u2'",
					"CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id TEXT NOT NULL, title TEXT NOT NULL, body TEXT, published INTEGER NOT NULL)",
					"CREATE INDEX posts_user_id_idx ON posts(user_id)",
					"INSERT INTO posts (id, user_id, title, body, published) VALUES (1, 'u1', 'hello', 'first body', 0)",
					"INSERT INTO posts (id, user_id, title, body, published) VALUES (2, 'u1', 'second', 'second body', 1)",
					"UPDATE posts SET body = 'first body updated', published = 1 WHERE id = 1",
					"DELETE FROM posts WHERE id = 2",
				],
				(statement) =>
					Effect.tryPromise(() => sourceDb.exec(statement)).pipe(Effect.asVoid),
				{ concurrency: 1, discard: true },
			);

			const [changes, sourceSnapshot, targetSnapshot] = yield* replayAndSnapshot(
				sourceDb,
				targetDb,
			);

			expect(changes.length).toBeGreaterThan(0);
			expect(targetSnapshot).toEqual(sourceSnapshot);
		}),
	);

	it.effect("replays dropped indexes and later schema additions", () =>
		Effect.gen(function* () {
			yield* Effect.forEach(
				[
					"CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL, image BLOB)",
					"CREATE INDEX products_sku_idx ON products(sku)",
					"INSERT INTO products (id, sku, price, stock, image) VALUES (1, 'sku-1', 19.99, 5, x'AA')",
					"INSERT INTO products (id, sku, price, stock, image) VALUES (2, 'sku-2', 29.99, 9, x'BBCC')",
					"UPDATE products SET price = 24.5, stock = 7 WHERE id = 1",
					"ALTER TABLE products ADD COLUMN description TEXT",
					"UPDATE products SET description = 'featured' WHERE id = 1",
					"DROP INDEX products_sku_idx",
					"DELETE FROM products WHERE id = 2",
					"INSERT INTO products (id, sku, price, stock, image, description) VALUES (3, 'sku-3', 49.5, 2, x'DD', 'new')",
					"CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT, enabled INTEGER NOT NULL)",
					"INSERT INTO settings (key, value, enabled) VALUES ('theme', 'dark', 1)",
					"INSERT INTO settings (key, value, enabled) VALUES ('beta', NULL, 0)",
					"UPDATE settings SET value = 'on', enabled = 1 WHERE key = 'beta'",
				],
				(statement) =>
					Effect.tryPromise(() => sourceDb.exec(statement)).pipe(Effect.asVoid),
				{ concurrency: 1, discard: true },
			);

			const [_changes, sourceSnapshot, targetSnapshot] = yield* replayAndSnapshot(
				sourceDb,
				targetDb,
			);

			expect(targetSnapshot).toEqual(sourceSnapshot);
		}),
	);
});
