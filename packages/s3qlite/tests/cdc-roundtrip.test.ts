import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "@effect/vitest";
import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";

import { replayCDC } from "../src/cdc/apply";
import { extractCDC } from "../src/cdc/extract";
import { snapshotDatabase } from "../src/cdc/testUtils";

describe("cdc round-trip", () => {
	let baseDir: string;
	let source: Database;
	let target: Database;

	beforeAll(async () => {
		baseDir = await mkdtemp("/dev/shm/s3qlite-cdc-rt-");
	});

	afterAll(async () => {
		await rm(baseDir, { recursive: true, force: true });
	});

	beforeEach(async () => {
		source = await connectTurso(join(baseDir, `${crypto.randomUUID()}.sqlite`));
		target = await connectTurso(join(baseDir, `${crypto.randomUUID()}.sqlite`));
		await source.exec("PRAGMA capture_data_changes_conn('full')");
	});

	afterEach(async () => {
		await Promise.all([source.close(), target.close()]);
	});

	it.effect("extract → replay reproduces identical data rows", () =>
		Effect.gen(function* () {
			// seed: create table, insert 3 rows, update 1, delete 1
			yield* Effect.forEach(
				[
					"CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL)",
					"INSERT INTO items (id, label, price, stock) VALUES (1, 'alpha', 10.0, 100)",
					"INSERT INTO items (id, label, price, stock) VALUES (2, 'beta',  20.5,  50)",
					"INSERT INTO items (id, label, price, stock) VALUES (3, 'gamma', 30.0,   0)",
					"UPDATE items SET price = 12.5, stock = 90 WHERE id = 1",
					"DELETE FROM items WHERE id = 2",
				],
				(stmt) => Effect.tryPromise(() => source.exec(stmt)).pipe(Effect.asVoid),
				{ concurrency: 1, discard: true },
			);

			// verify seed result before extraction
			const rows = yield* Effect.tryPromise(
				() =>
					source.all("SELECT id, label, price, stock FROM items ORDER BY id") as Promise<
						unknown[]
					>,
			);
			expect(rows).toEqual([
				{ id: 1, label: "alpha", price: 12.5, stock: 90 },
				{ id: 3, label: "gamma", price: 30.0, stock: 0 },
			]);

			// extract from source, replay onto target
			const changes = yield* extractCDC(source, 0);
			expect(changes.length).toBeGreaterThan(0);

			yield* replayCDC(target, changes);

			// compare
			const [sourceSnap, targetSnap] = yield* Effect.all(
				[snapshotDatabase(source), snapshotDatabase(target)],
				{ concurrency: 1 },
			);
			expect(targetSnap).toEqual(sourceSnap);
		}),
	);
});