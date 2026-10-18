import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";

import { replayCDC } from "../src/cdc/apply";
import { extractCDC } from "../src/cdc/extract";
import { deserializeCDC, serializeCDC } from "../src/cdc/protobuf";
import { snapshotDatabase } from "../src/cdc/testUtils";

// Regression: replay used the after-image to locate the row of an UPDATE, so an UPDATE that
// changed the primary key matched no row and was silently lost.
describe("CDC replay of primary key updates", () => {
	let directory: string;
	let source: Database;
	let target: Database;

	beforeEach(async () => {
		directory = await mkdtemp("/dev/shm/s3qlite-pk-update-");
		source = await connectTurso(join(directory, "source.db"));
		target = await connectTurso(join(directory, "target.db"));
		await source.exec("PRAGMA capture_data_changes_conn('full')");
	});

	afterEach(async () => {
		await Promise.all([source.close(), target.close()]);
		await rm(directory, { recursive: true, force: true });
	});

	const replayStatements = (statements: readonly string[]) =>
		Effect.gen(function* () {
			for (const statement of statements) {
				yield* Effect.promise(() => source.exec(statement));
			}
			// Replay what a batch carries: the extracted rows after a protobuf round trip.
			const changes = deserializeCDC(serializeCDC(yield* extractCDC(source, 0)));
			yield* replayCDC(target, changes);
			return {
				source: yield* snapshotDatabase(source),
				target: yield* snapshotDatabase(target),
			};
		});

	it.effect("replays a changed TEXT primary key", () =>
		Effect.gen(function* () {
			const { source, target } = yield* replayStatements([
				"CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)",
				"INSERT INTO users VALUES ('a', 'alice'), ('b', 'bob')",
				"UPDATE users SET id = 'z' WHERE id = 'a'",
				"UPDATE users SET id = 'y', name = 'bobby' WHERE id = 'b'",
			]);
			expect(target).toEqual(source);
			expect(target.tables.users).toEqual([
				{ id: "y", name: "bobby" },
				{ id: "z", name: "alice" },
			]);
		}),
	);

	it.effect("replays a changed INTEGER PRIMARY KEY (rowid alias)", () =>
		Effect.gen(function* () {
			const { source, target } = yield* replayStatements([
				"CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
				"INSERT INTO items VALUES (1, 'one'), (2, 'two')",
				"UPDATE items SET id = 10 WHERE id = 1",
				"UPDATE items SET id = id + 100",
			]);
			expect(target).toEqual(source);
			expect(target.tables.items).toEqual([
				{ id: 102, label: "two" },
				{ id: 110, label: "one" },
			]);
		}),
	);

	it.effect("replays a change of one component of a composite primary key", () =>
		Effect.gen(function* () {
			const { source, target } = yield* replayStatements([
				"CREATE TABLE memberships (org TEXT NOT NULL, member TEXT NOT NULL, role TEXT NOT NULL, PRIMARY KEY (org, member))",
				"INSERT INTO memberships VALUES ('o1', 'm1', 'owner'), ('o1', 'm2', 'viewer'), ('o2', 'm1', 'admin')",
				"UPDATE memberships SET member = 'm3' WHERE org = 'o1' AND member = 'm2'",
				"UPDATE memberships SET org = 'o3', role = 'owner' WHERE org = 'o2' AND member = 'm1'",
				"UPDATE memberships SET org = 'o9', member = 'm9' WHERE org = 'o1' AND member = 'm1'",
			]);
			expect(target).toEqual(source);
			expect(target.tables.memberships).toEqual([
				{ org: "o1", member: "m3", role: "viewer" },
				{ org: "o3", member: "m1", role: "owner" },
				{ org: "o9", member: "m9", role: "owner" },
			]);
		}),
	);

	it.effect("replays a primary key moved to a key that was freed in the same batch", () =>
		Effect.gen(function* () {
			const { source, target } = yield* replayStatements([
				"CREATE TABLE slots (id INTEGER PRIMARY KEY, holder TEXT NOT NULL)",
				"INSERT INTO slots VALUES (1, 'a'), (2, 'b')",
				"DELETE FROM slots WHERE id = 1",
				"UPDATE slots SET id = 1 WHERE id = 2",
				"INSERT INTO slots VALUES (2, 'c')",
			]);
			expect(target).toEqual(source);
		}),
	);
});