import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";
import fc from "fast-check";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "vitest";

import { connect } from "../src/connection";
import type { CloneTrait, ConnectDatabaseTrait, KV } from "../src/kv/kv";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";
import type { S3qliteDatabase } from "../src/types";

const instanceIds = ["a", "b", "c"] as const;

type InstanceId = (typeof instanceIds)[number];

type Action =
	| { kind: "connect"; instanceId: InstanceId }
	| { kind: "close"; instanceId: InstanceId }
	| { kind: "stabilize" }
	| {
			kind: "sqlCall";
			instanceId: InstanceId;
			op: "insert" | "update" | "delete";
			slot: number;
			value: number;
	  }
	| {
			kind: "syncCall";
			instanceId: InstanceId;
			op: "insert" | "update" | "delete";
			slot: number;
			value: number;
	  };

type LocalStore = KV & CloneTrait & ConnectDatabaseTrait;

type Harness = {
	dbName: string;
	control: Database;
	remoteKV: LocalStore;
	localKVs: Map<InstanceId, LocalStore>;
	open: Map<InstanceId, S3qliteDatabase>;
};

const actionArb: fc.Arbitrary<Action> = fc.oneof(
	fc.constant<Action>({ kind: "stabilize" }),
	fc.record({
		kind: fc.constant("connect"),
		instanceId: fc.constantFrom(...instanceIds),
	}) as fc.Arbitrary<Action>,
	fc.record({
		kind: fc.constant("close"),
		instanceId: fc.constantFrom(...instanceIds),
	}) as fc.Arbitrary<Action>,
	fc.record({
		kind: fc.constant("sqlCall"),
		instanceId: fc.constantFrom(...instanceIds),
		op: fc.constantFrom("insert" as const, "update" as const, "delete" as const),
		slot: fc.integer({ min: 0, max: 4 }),
		value: fc.integer({ min: 0, max: 999 }),
	}) as fc.Arbitrary<Action>,
	fc.record({
		kind: fc.constant("syncCall"),
		instanceId: fc.constantFrom(...instanceIds),
		op: fc.constantFrom("insert" as const, "update" as const, "delete" as const),
		slot: fc.integer({ min: 0, max: 4 }),
		value: fc.integer({ min: 0, max: 999 }),
	}) as fc.Arbitrary<Action>,
);

const schemaSql =
	"CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL)";

// oxlint-disable-next-line local-rules/function-minimum-length
const readRows = async (db: Database): Promise<unknown[]> => {
	const rows = await db.all("SELECT id, value, version FROM items ORDER BY id");
	return rows as unknown[];
};

const toSql = (
	action: Extract<Action, { kind: "sqlCall" }> | Extract<Action, { kind: "syncCall" }>,
): string => {
	const id = `${action.instanceId}-${action.slot}`;
	const value = `v${action.value}`;

	if (action.op === "insert") {
		return `INSERT OR REPLACE INTO items (id, value, version) VALUES ('${id}', '${value}', ${action.value})`;
	}

	if (action.op === "update") {
		return `UPDATE items SET value = '${value}', version = version + 1 WHERE id = '${id}'`;
	}

	return `DELETE FROM items WHERE id = '${id}'`;
};

const getLocalKV = (harness: Harness, instanceId: InstanceId) =>
	Effect.gen(function* () {
		const existing = harness.localKVs.get(instanceId);
		if (existing) {
			return existing;
		}

		const localKV = yield* makeMemoryKV();
		harness.localKVs.set(instanceId, localKV);
		return localKV;
	});

const openInstance = (harness: Harness, instanceId: InstanceId) =>
	Effect.gen(function* () {
		const existing = harness.open.get(instanceId);
		if (existing) {
			return existing;
		}

		const db = yield* connect(harness.dbName, { bucket: "test" }).pipe(
			Effect.provideService(RemoteKV, harness.remoteKV),
			Effect.provideService(LocalKV, yield* getLocalKV(harness, instanceId)),
		);
		harness.open.set(instanceId, db);
		return db;
	});

const closeInstance = (harness: Harness, instanceId: InstanceId) =>
	Effect.gen(function* () {
		const db = harness.open.get(instanceId);
		if (!db) {
			return;
		}

		yield* Effect.promise(() => db.close());
		harness.open.delete(instanceId);
	});

const stabilize = (harness: Harness) =>
	Effect.gen(function* () {
		for (let round = 0; round < 2; round++) {
			for (const instanceId of instanceIds) {
				const db = yield* openInstance(harness, instanceId);
				yield* db.pull();
				yield* db.push();
			}
		}

		const expected = yield* Effect.promise(() => readRows(harness.control));

		for (const instanceId of instanceIds) {
			const wasOpen = harness.open.has(instanceId);
			const db = yield* openInstance(harness, instanceId);
			const actual = yield* Effect.promise(() => readRows(db));
			if (JSON.stringify(actual) !== JSON.stringify(expected)) {
				throw new Error(
					JSON.stringify({
						instanceId,
						actual,
						expected,
					}),
				);
			}
			if (!wasOpen) {
				yield* closeInstance(harness, instanceId);
			}
		}
	});

const runCase = (actions: readonly Action[]) =>
	Effect.gen(function* () {
		const controlDir = yield* Effect.acquireRelease(
			Effect.promise(() => mkdtemp(join(tmpdir(), "s3qlite-fuzzy-"))),
			(dir) =>
				Effect.promise(() => rm(dir, { recursive: true, force: true })).pipe(Effect.orDie),
		);
		const harness: Harness = {
			dbName: `fuzzy-${crypto.randomUUID()}`,
			control: yield* Effect.acquireRelease(
				Effect.promise(() => connectTurso(join(controlDir, "control.db"))),
				(db) => Effect.promise(() => db.close()).pipe(Effect.orDie),
			),
			remoteKV: yield* makeMemoryKV(),
			localKVs: new Map(),
			open: new Map(),
		};

		const seed = yield* openInstance(harness, "a");
		// No AUTOINCREMENT here: per-replica generated ids diverge across sync until we implement a dedicated workaround.
		yield* Effect.promise(() => seed.exec(schemaSql));
		yield* Effect.promise(() => harness.control.exec(schemaSql));
		yield* seed.push();

		for (const action of actions) {
			if (action.kind === "connect") {
				yield* openInstance(harness, action.instanceId);
				continue;
			}

			if (action.kind === "close") {
				yield* closeInstance(harness, action.instanceId);
				continue;
			}

			if (action.kind === "stabilize") {
				yield* stabilize(harness);
				continue;
			}

			const db = yield* openInstance(harness, action.instanceId);
			const sql = toSql(action);
			const phase = action.kind;
			yield* Effect.promise(() => db.run(sql));
			yield* Effect.promise(() => harness.control.run(sql));
			if (phase === "syncCall") {
				yield* db.sync();
			} else {
				yield* db.pull();
				yield* db.push();
			}
			const actual = yield* Effect.promise(() => readRows(db));
			const expected = yield* Effect.promise(() => readRows(harness.control));
			if (JSON.stringify(actual) !== JSON.stringify(expected)) {
				throw new Error(
					JSON.stringify({
						phase,
						instanceId: action.instanceId,
						sql,
						actual,
						expected,
					}),
				);
			}
		}

		yield* stabilize(harness);
		for (const instanceId of instanceIds) {
			yield* closeInstance(harness, instanceId);
		}
	});

describe("s3qlite fuzzy sync", () => {
	it("matches a control database after stabilization", async () => {
		await fc.assert(
			fc.asyncProperty(
				fc.array(actionArb, { minLength: 12, maxLength: 40 }),
				async (actions) => {
					await Effect.runPromise(
						Effect.scoped(runCase(actions)).pipe(Effect.provide(fileSystemLayer)),
					);
				},
			),
			{ numRuns: 100 },
		);
	});
});
