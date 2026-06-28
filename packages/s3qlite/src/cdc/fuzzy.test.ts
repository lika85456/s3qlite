import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";
import fc from "fast-check";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { replayCDC } from "./apply";
import { extractCDC } from "./extract";
import { applyBatch, snapshotDatabase } from "./testUtils";
import type { CDCValue } from "./types";

type ColumnType = "INTEGER" | "REAL" | "TEXT" | "BLOB";
// oxlint-disable-next-line local-rules/no-null-undefined-option -- the fuzzy model needs SQL NULL values.
type SqlValue = Exclude<CDCValue, null> | null;

type ColumnSeed = {
	type: ColumnType;
	nullable: boolean;
	defaulted: boolean;
};

type CreateTableSeed = {
	kind: "createTable";
	pkMode: "none" | "integer" | "text";
	columns: readonly ColumnSeed[];
};

type DropTableSeed = {
	kind: "dropTable";
	target: number;
};

type AddColumnSeed = {
	kind: "addColumn";
	target: number;
	column: ColumnSeed;
	salt: number;
};

type CreateIndexSeed = {
	kind: "createIndex";
	target: number;
	column: number;
};

type DropIndexSeed = {
	kind: "dropIndex";
	target: number;
};

type CreateViewSeed = {
	kind: "createView";
	target: number;
	column: number;
	width: number;
};

type DropViewSeed = {
	kind: "dropView";
	target: number;
};

type CreateTriggerSeed = {
	kind: "createTrigger";
	target: number;
	event: "INSERT" | "UPDATE" | "DELETE";
	column: number;
};

type DropTriggerSeed = {
	kind: "dropTrigger";
	target: number;
};

type InsertSeed = {
	kind: "insert";
	target: number;
	salt: number;
};

type UpdateSeed = {
	kind: "update";
	target: number;
	row: number;
	mask: number;
	salt: number;
};

type DeleteSeed = {
	kind: "delete";
	target: number;
	row: number;
};

type ActionSeed =
	| CreateTableSeed
	| DropTableSeed
	| AddColumnSeed
	| CreateIndexSeed
	| DropIndexSeed
	| CreateViewSeed
	| DropViewSeed
	| CreateTriggerSeed
	| DropTriggerSeed
	| InsertSeed
	| UpdateSeed
	| DeleteSeed;

type BatchSeed = {
	transactional: boolean;
	actions: readonly ActionSeed[];
};

type ColumnModel = {
	name: string;
	type: ColumnType;
	nullable: boolean;
	primaryKey: boolean;
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- the model tracks absence of a DEFAULT clause separately from SQL NULL.
	defaultValue: SqlValue | undefined;
};

type RowModel = {
	rowId: number;
	values: Record<string, SqlValue>;
};

type TableModel = {
	name: string;
	columns: ColumnModel[];
	rows: RowModel[];
	nextRowId: number;
	nextIntegerPk: number;
	nextTextPk: number;
};

type NamedTableObject = {
	name: string;
	tableName: string;
};

type StateModel = {
	tables: TableModel[];
	indexes: NamedTableObject[];
	views: NamedTableObject[];
	triggers: NamedTableObject[];
	nextTableId: number;
	nextColumnId: number;
	nextIndexId: number;
	nextViewId: number;
	nextTriggerId: number;
};

type SqlBatch = {
	transactional: boolean;
	statements: readonly string[];
};

const columnTypeArb = fc.constantFrom<ColumnType>("INTEGER", "REAL", "TEXT", "BLOB");

const columnSeedArb = fc.record({
	type: columnTypeArb,
	nullable: fc.boolean(),
	defaulted: fc.boolean(),
});

const actionSeedArb: fc.Arbitrary<ActionSeed> = fc.oneof(
	fc.record({
		kind: fc.constant<CreateTableSeed["kind"]>("createTable"),
		pkMode: fc.constantFrom<CreateTableSeed["pkMode"]>("none", "integer", "text"),
		columns: fc.array(columnSeedArb, { minLength: 1, maxLength: 4 }),
	}),
	fc.record({ kind: fc.constant<DropTableSeed["kind"]>("dropTable"), target: fc.nat(31) }),
	fc.record({
		kind: fc.constant<AddColumnSeed["kind"]>("addColumn"),
		target: fc.nat(31),
		column: columnSeedArb,
		salt: fc.integer(),
	}),
	fc.record({
		kind: fc.constant<CreateIndexSeed["kind"]>("createIndex"),
		target: fc.nat(31),
		column: fc.nat(31),
	}),
	fc.record({ kind: fc.constant<DropIndexSeed["kind"]>("dropIndex"), target: fc.nat(31) }),
	fc.record({
		kind: fc.constant<InsertSeed["kind"]>("insert"),
		target: fc.nat(31),
		salt: fc.integer(),
	}),
	fc.record({
		kind: fc.constant<UpdateSeed["kind"]>("update"),
		target: fc.nat(31),
		row: fc.nat(127),
		mask: fc.integer({ min: 1, max: 255 }),
		salt: fc.integer(),
	}),
	fc.record({
		kind: fc.constant<DeleteSeed["kind"]>("delete"),
		target: fc.nat(31),
		row: fc.nat(127),
	}),
);

const batchSeedArb = fc.record({
	transactional: fc.boolean(),
	actions: fc.array(actionSeedArb, { minLength: 1, maxLength: 6 }),
});

const programArb = fc
	.array(batchSeedArb, { minLength: 18, maxLength: 54 })
	.map((batches) => buildProgram(batches))
	.filter((batches) => batches.length > 0);

// oxlint-disable-next-line local-rules/no-null-undefined-option, local-rules/function-minimum-length -- this fuzz primitive intentionally returns a missing-item sentinel and is reused throughout the generated state machine.
const pick = <A>(items: readonly A[], seed: number): A | undefined =>
	items[Math.abs(seed) % items.length];

// oxlint-disable-next-line local-rules/function-minimum-length -- SQL identifier quoting is a tiny, heavily reused primitive in the fuzz model.
const identifier = (value: string): string => `"${value.replaceAll('"', '""')}"`;

const literal = (value: SqlValue): string => {
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- generated SQL literals must include SQL NULL.
	if (value === null) {
		return "NULL";
	}
	if (value instanceof Uint8Array) {
		return `x'${Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("")}'`;
	}
	if (typeof value === "string") {
		return `'${value.replaceAll("'", "''")}'`;
	}
	return Number.isFinite(value) ? `${value}` : "0";
};

// oxlint-disable-next-line local-rules/no-null-undefined-option -- this helper intentionally excludes SQL NULL from generated scalar values.
const createScalar = (type: ColumnType, salt: number): Exclude<SqlValue, null> => {
	switch (type) {
		case "INTEGER":
			return (Math.abs(salt) % 2001) - 1000;
		case "REAL":
			return Math.round((((Math.abs(salt) % 20001) - 10000) / 37) * 1000) / 1000;
		case "TEXT":
			return `txt_${Math.abs(salt)}_'_${Math.abs(salt % 17)}`;
		case "BLOB":
			return Uint8Array.from(
				{ length: (Math.abs(salt) % 4) + 1 },
				(_, index) => (Math.abs(salt) + index * 53) % 256,
			);
	}
};

// oxlint-disable-next-line local-rules/function-minimum-length -- nullable SQL value generation is a central fuzz-model primitive used by multiple actions.
const createValue = (column: ColumnModel, salt: number, allowNull: boolean): SqlValue =>
	allowNull && column.nullable && Math.abs(salt) % 5 === 0
		? // oxlint-disable-next-line local-rules/no-null-undefined-option -- the fuzzy model intentionally generates SQL NULL values.
			null
		: createScalar(column.type, salt);

const initialState = (): StateModel => ({
	tables: [],
	indexes: [],
	views: [],
	triggers: [],
	nextTableId: 1,
	nextColumnId: 1,
	nextIndexId: 1,
	nextViewId: 1,
	nextTriggerId: 1,
});

const createColumnSql = (column: ColumnModel): string => {
	const parts = [identifier(column.name), column.type];
	if (column.primaryKey) {
		parts.push("PRIMARY KEY");
	}
	if (!column.nullable || column.primaryKey) {
		parts.push("NOT NULL");
	}
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- omitting DEFAULT is distinct from DEFAULT NULL in generated schemas.
	if (column.defaultValue !== undefined) {
		parts.push(`DEFAULT ${literal(column.defaultValue)}`);
	}
	return parts.join(" ");
};

const createTable = (state: StateModel, seed: CreateTableSeed): readonly string[] => {
	const table: TableModel = {
		name: `t_${state.nextTableId++}`,
		columns: [],
		rows: [],
		nextRowId: 1,
		nextIntegerPk: 1,
		nextTextPk: 1,
	};
	if (seed.pkMode === "integer") {
		table.columns.push({
			name: `id_${state.nextColumnId++}`,
			type: "INTEGER",
			nullable: false,
			primaryKey: true,
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- primary-key seed columns intentionally omit a DEFAULT clause.
			defaultValue: undefined,
		});
	}
	if (seed.pkMode === "text") {
		table.columns.push({
			name: `id_${state.nextColumnId++}`,
			type: "TEXT",
			nullable: false,
			primaryKey: true,
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- primary-key seed columns intentionally omit a DEFAULT clause.
			defaultValue: undefined,
		});
	}
	for (const column of seed.columns) {
		table.columns.push({
			name: `c_${state.nextColumnId++}`,
			type: column.type,
			nullable: column.nullable,
			primaryKey: false,
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- generated columns intentionally omit a DEFAULT clause unless requested.
			defaultValue: undefined,
		});
	}
	state.tables.push(table);
	return [
		`CREATE TABLE ${identifier(table.name)} (${table.columns.map(createColumnSql).join(", ")})`,
	];
};

const dropTable = (state: StateModel, seed: DropTableSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table) {
		return [];
	}
	const statements = [
		...state.views
			.filter((view) => view.tableName === table.name)
			.map((view) => `DROP VIEW ${identifier(view.name)}`),
		...state.triggers
			.filter((trigger) => trigger.tableName === table.name)
			.map((trigger) => `DROP TRIGGER ${identifier(trigger.name)}`),
		...state.indexes
			.filter((index) => index.tableName === table.name)
			.map((index) => `DROP INDEX ${identifier(index.name)}`),
		`DROP TABLE ${identifier(table.name)}`,
	];
	state.tables = state.tables.filter((item) => item.name !== table.name);
	state.views = state.views.filter((item) => item.tableName !== table.name);
	state.triggers = state.triggers.filter((item) => item.tableName !== table.name);
	state.indexes = state.indexes.filter((item) => item.tableName !== table.name);
	return statements;
};

const addColumn = (state: StateModel, seed: AddColumnSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table) {
		return [];
	}
	const defaultValue = seed.column.defaulted
		? createScalar(seed.column.type, seed.salt)
		: // oxlint-disable-next-line local-rules/no-null-undefined-option -- omitting DEFAULT is distinct from DEFAULT NULL in generated schemas.
			undefined;
	const column: ColumnModel = {
		name: `c_${state.nextColumnId++}`,
		type: seed.column.type,
		// oxlint-disable-next-line local-rules/no-null-undefined-option -- omitted DEFAULT means SQLite backfills NULL and keeps the column nullable.
		nullable: defaultValue === undefined ? true : seed.column.nullable,
		primaryKey: false,
		defaultValue,
	};
	table.columns.push(column);
	for (const row of table.rows) {
		// oxlint-disable-next-line local-rules/no-null-undefined-option -- SQLite fills existing rows with NULL when no DEFAULT is declared.
		row.values[column.name] = defaultValue ?? null;
	}
	return [`ALTER TABLE ${identifier(table.name)} ADD COLUMN ${createColumnSql(column)}`];
};

const createIndex = (state: StateModel, seed: CreateIndexSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table) {
		return [];
	}
	const column = pick(table.columns, seed.column);
	if (!column) {
		return [];
	}
	const index = { name: `idx_${state.nextIndexId++}`, tableName: table.name };
	state.indexes.push(index);
	return [
		`CREATE INDEX ${identifier(index.name)} ON ${identifier(table.name)} (${identifier(column.name)})`,
	];
};

const dropIndex = (state: StateModel, seed: DropIndexSeed): readonly string[] => {
	const index = pick(state.indexes, seed.target);
	if (!index) {
		return [];
	}
	state.indexes = state.indexes.filter((item) => item.name !== index.name);
	return [`DROP INDEX ${identifier(index.name)}`];
};

const createView = (state: StateModel, seed: CreateViewSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table || table.columns.length === 0) {
		return [];
	}
	const start = Math.abs(seed.column) % table.columns.length;
	const columns = Array.from(
		{ length: Math.min(seed.width, table.columns.length) },
		(_, index) => table.columns[(start + index) % table.columns.length],
	);
	const view = { name: `view_${state.nextViewId++}`, tableName: table.name };
	state.views.push(view);
	return [
		`CREATE VIEW ${identifier(view.name)} AS SELECT ${columns.map((column) => identifier(column.name)).join(", ")} FROM ${identifier(table.name)}`,
	];
};

const dropView = (state: StateModel, seed: DropViewSeed): readonly string[] => {
	const view = pick(state.views, seed.target);
	if (!view) {
		return [];
	}
	state.views = state.views.filter((item) => item.name !== view.name);
	return [`DROP VIEW ${identifier(view.name)}`];
};

const createTrigger = (state: StateModel, seed: CreateTriggerSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table || table.columns.length === 0) {
		return [];
	}
	const column = pick(table.columns, seed.column);
	if (!column) {
		return [];
	}
	const trigger = { name: `trigger_${state.nextTriggerId++}`, tableName: table.name };
	state.triggers.push(trigger);
	const scope = seed.event === "DELETE" ? "OLD" : "NEW";
	return [
		`CREATE TRIGGER ${identifier(trigger.name)} AFTER ${seed.event} ON ${identifier(table.name)} BEGIN SELECT ${scope}.${identifier(column.name)}; END`,
	];
};

const dropTrigger = (state: StateModel, seed: DropTriggerSeed): readonly string[] => {
	const trigger = pick(state.triggers, seed.target);
	if (!trigger) {
		return [];
	}
	state.triggers = state.triggers.filter((item) => item.name !== trigger.name);
	return [`DROP TRIGGER ${identifier(trigger.name)}`];
};

const insertRow = (state: StateModel, seed: InsertSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table) {
		return [];
	}
	const rowValues: Record<string, SqlValue> = {};
	let rowId = table.nextRowId++;
	for (const [index, column] of table.columns.entries()) {
		const value = column.primaryKey
			? column.type === "INTEGER"
				? table.nextIntegerPk++
				: `${table.name}_pk_${table.nextTextPk++}`
			: createValue(column, seed.salt + index * 101, true);
		rowValues[column.name] = value;
		if (column.primaryKey && column.type === "INTEGER" && typeof value === "number") {
			rowId = value;
		}
	}
	table.rows.push({ rowId, values: rowValues });
	return [
		// oxlint-disable-next-line local-rules/no-null-undefined-option -- missing generated values must serialize as SQL NULL.
		`INSERT INTO ${identifier(table.name)} (${table.columns.map((column) => identifier(column.name)).join(", ")}) VALUES (${table.columns.map((column) => literal(rowValues[column.name] ?? null)).join(", ")})`,
	];
};

const updateRow = (state: StateModel, seed: UpdateSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table || table.rows.length === 0) {
		return [];
	}
	const mutableColumns = table.columns.filter((column) => !column.primaryKey);
	if (mutableColumns.length === 0) {
		return [];
	}
	const row = pick(table.rows, seed.row);
	if (!row) {
		return [];
	}
	const selected = mutableColumns.filter((_, index) => ((seed.mask >> (index % 8)) & 1) === 1);
	const columns =
		selected.length > 0
			? selected
			: [mutableColumns[Math.abs(seed.mask) % mutableColumns.length]];
	for (const [index, column] of columns.entries()) {
		row.values[column.name] = createValue(column, seed.salt + index * 151, true);
	}
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- tables without explicit primary keys fall back to rowid addressing.
	const primaryKey = table.columns.find((column) => column.primaryKey) ?? null;
	const whereSql = primaryKey
		? // oxlint-disable-next-line local-rules/no-null-undefined-option -- missing generated values must serialize as SQL NULL.
			`${identifier(primaryKey.name)} = ${literal(row.values[primaryKey.name] ?? null)}`
		: `rowid = ${row.rowId}`;
	return [
		// oxlint-disable-next-line local-rules/no-null-undefined-option -- missing generated values must serialize as SQL NULL.
		`UPDATE ${identifier(table.name)} SET ${columns.map((column) => `${identifier(column.name)} = ${literal(row.values[column.name] ?? null)}`).join(", ")} WHERE ${whereSql}`,
	];
};

const deleteRow = (state: StateModel, seed: DeleteSeed): readonly string[] => {
	const table = pick(state.tables, seed.target);
	if (!table || table.rows.length === 0) {
		return [];
	}
	const row = pick(table.rows, seed.row);
	if (!row) {
		return [];
	}
	table.rows = table.rows.filter((item) => item !== row);
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- tables without explicit primary keys fall back to rowid addressing.
	const primaryKey = table.columns.find((column) => column.primaryKey) ?? null;
	const whereSql = primaryKey
		? // oxlint-disable-next-line local-rules/no-null-undefined-option -- missing generated values must serialize as SQL NULL.
			`${identifier(primaryKey.name)} = ${literal(row.values[primaryKey.name] ?? null)}`
		: `rowid = ${row.rowId}`;
	return [`DELETE FROM ${identifier(table.name)} WHERE ${whereSql}`];
};

const buildAction = (state: StateModel, seed: ActionSeed): readonly string[] => {
	switch (seed.kind) {
		case "createTable":
			return createTable(state, seed);
		case "dropTable":
			return dropTable(state, seed);
		case "addColumn":
			return addColumn(state, seed);
		case "createIndex":
			return createIndex(state, seed);
		case "dropIndex":
			return dropIndex(state, seed);
		case "createView":
			return createView(state, seed);
		case "dropView":
			return dropView(state, seed);
		case "createTrigger":
			return createTrigger(state, seed);
		case "dropTrigger":
			return dropTrigger(state, seed);
		case "insert":
			return insertRow(state, seed);
		case "update":
			return updateRow(state, seed);
		case "delete":
			return deleteRow(state, seed);
	}
};

const buildProgram = (seeds: readonly BatchSeed[]): readonly SqlBatch[] => {
	const state = initialState();
	const batches: SqlBatch[] = [];
	for (const seed of seeds) {
		const statements = seed.actions.flatMap((action) => buildAction(state, action));
		if (statements.length > 0) {
			batches.push({ transactional: seed.transactional, statements });
		}
	}
	return batches;
};

const createDatabasePair = async (baseDir: string): Promise<readonly [Database, Database]> => {
	const [sourcePath, targetPath] = [
		join(baseDir, `${crypto.randomUUID()}-source.sqlite`),
		join(baseDir, `${crypto.randomUUID()}-target.sqlite`),
	];
	const sourceDb = await connectTurso(sourcePath);
	const targetDb = await connectTurso(targetPath);
	await sourceDb.exec("PRAGMA capture_data_changes_conn('full')");
	return [sourceDb, targetDb] as const;
};

const runIncrementalReplay = async (
	baseDir: string,
	program: readonly SqlBatch[],
): Promise<void> => {
	const [sourceDb, targetDb] = await createDatabasePair(baseDir);
	try {
		let lastChangeId = 0;
		for (const batch of program) {
			await Effect.runPromise(applyBatch(sourceDb, batch));
			const changes = await Effect.runPromise(extractCDC(sourceDb, lastChangeId));
			if (changes.length > 0) {
				lastChangeId = changes[changes.length - 1]?.changeId ?? lastChangeId;
				await Effect.runPromise(replayCDC(targetDb, changes));
			}
			const [sourceSnapshot, targetSnapshot] = await Promise.all([
				Effect.runPromise(snapshotDatabase(sourceDb)),
				Effect.runPromise(snapshotDatabase(targetDb)),
			]);
			expect(targetSnapshot).toEqual(sourceSnapshot);
		}
	} finally {
		await Promise.all([sourceDb.close(), targetDb.close()]);
	}
};

const runFullReplay = async (baseDir: string, program: readonly SqlBatch[]): Promise<void> => {
	const [sourceDb, targetDb] = await createDatabasePair(baseDir);
	try {
		for (const batch of program) {
			await Effect.runPromise(applyBatch(sourceDb, batch));
		}
		const changes = await Effect.runPromise(extractCDC(sourceDb, 0));
		expect(changes.length).toBeGreaterThan(0);
		await Effect.runPromise(replayCDC(targetDb, changes));
		const [sourceSnapshot, targetSnapshot] = await Promise.all([
			Effect.runPromise(snapshotDatabase(sourceDb)),
			Effect.runPromise(snapshotDatabase(targetDb)),
		]);
		expect(targetSnapshot).toEqual(sourceSnapshot);
	} finally {
		await Promise.all([sourceDb.close(), targetDb.close()]);
	}
};

describe.skip("cdc fuzzy", () => {
	let baseDir: string;

	beforeAll(async () => {
		baseDir = await mkdtemp("/dev/shm/s3qlite-cdc-fuzzy-");
	});

	afterAll(async () => {
		await rm(baseDir, { recursive: true, force: true });
	});

	it.concurrent("replays generated SQL programs incrementally", async () => {
		await fc.assert(
			fc.asyncProperty(programArb, (program) => runIncrementalReplay(baseDir, program)),
			{
				numRuns: 256,
			},
		);
	}, 300000);

	it.concurrent("replays generated SQL programs from full CDC streams", async () => {
		await fc.assert(
			fc.asyncProperty(programArb, (program) => runFullReplay(baseDir, program)),
			{
				numRuns: 256,
			},
		);
	}, 300000);
});