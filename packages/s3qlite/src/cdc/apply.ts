import type { Database } from "@tursodatabase/database";
import { Data, Effect } from "effect";

import { CDCChangeType } from "./types";
import type { CDCRow, CDCValue } from "./types";

export class ReplayError extends Data.TaggedError("ReplayError")<{
	message: string;
	cause: unknown;
}> {}

type TableColumn = {
	name: string;
	pk: number;
};

type SchemaRow = {
	type: string;
	name: string;
	tblName: string;
	rootpage: number;
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- sqlite_schema CDC can carry NULL SQL for implicit objects.
	sql: string | null;
};

const textDecoder = new TextDecoder();

const readVarint = (bytes: Uint8Array, offset: number): readonly [number, number] => {
	let value = 0n;
	for (let index = 0; index < 9; index += 1) {
		const cursor = offset + index;
		if (cursor >= bytes.length) {
			throw new Error("Unexpected end of CDC record");
		}
		const byte = bytes[cursor];
		if (index === 8) {
			value = (value << 8n) | BigInt(byte);
			const number = Number(value);
			if (!Number.isSafeInteger(number)) {
				throw new Error("CDC varint is too large");
			}
			return [number, cursor + 1];
		}
		value = (value << 7n) | BigInt(byte & 0x7f);
		if ((byte & 0x80) === 0) {
			const number = Number(value);
			if (!Number.isSafeInteger(number)) {
				throw new Error("CDC varint is too large");
			}
			return [number, cursor + 1];
		}
	}
	throw new Error("Invalid CDC varint");
};

const readInteger = (bytes: Uint8Array, offset: number, length: number): number => {
	let value = 0n;
	for (let index = 0; index < length; index += 1) {
		const cursor = offset + index;
		if (cursor >= bytes.length) {
			throw new Error("Unexpected end of CDC integer");
		}
		value = (value << 8n) | BigInt(bytes[cursor]);
	}
	const bits = BigInt(length * 8);
	const signBit = 1n << (bits - 1n);
	const signed = (value & signBit) === 0n ? value : value - (1n << bits);
	const number = Number(signed);
	if (!Number.isSafeInteger(number)) {
		throw new Error("CDC integer is outside JS safe integer range");
	}
	return number;
};

const decodeValue = (
	bytes: Uint8Array,
	offset: number,
	serialType: number,
): readonly [CDCValue, number] => {
	switch (serialType) {
		case 0:
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- SQLite record serial type 0 represents SQL NULL.
			return [null, offset];
		case 1:
			return [readInteger(bytes, offset, 1), offset + 1];
		case 2:
			return [readInteger(bytes, offset, 2), offset + 2];
		case 3:
			return [readInteger(bytes, offset, 3), offset + 3];
		case 4:
			return [readInteger(bytes, offset, 4), offset + 4];
		case 5:
			return [readInteger(bytes, offset, 6), offset + 6];
		case 6:
			return [readInteger(bytes, offset, 8), offset + 8];
		case 7:
			if (offset + 8 > bytes.length) {
				throw new Error("Unexpected end of CDC float");
			}
			return [
				new DataView(bytes.buffer, bytes.byteOffset + offset, 8).getFloat64(0, false),
				offset + 8,
			];
		case 8:
			return [0, offset];
		case 9:
			return [1, offset];
		case 10:
		case 11:
			throw new Error(`Unsupported CDC serial type ${serialType}`);
		default: {
			const isBlob = serialType % 2 === 0;
			const length = isBlob ? (serialType - 12) / 2 : (serialType - 13) / 2;
			if (length < 0 || offset + length > bytes.length) {
				throw new Error("Unexpected end of CDC payload");
			}
			const value = bytes.slice(offset, offset + length);
			return [isBlob ? value : textDecoder.decode(value), offset + length];
		}
	}
};

const decodeRecord = (bytes: Uint8Array): Effect.Effect<readonly CDCValue[], Error> =>
	Effect.try(() => {
		const [headerSize, start] = readVarint(bytes, 0);
		if (headerSize > bytes.length) {
			throw new Error("CDC record header exceeds payload length");
		}
		const serialTypes: number[] = [];
		let headerOffset = start;
		while (headerOffset < headerSize) {
			const [serialType, nextOffset] = readVarint(bytes, headerOffset);
			serialTypes.push(serialType);
			headerOffset = nextOffset;
		}
		const values: CDCValue[] = [];
		let valueOffset = headerSize;
		for (const serialType of serialTypes) {
			const [value, nextOffset] = decodeValue(bytes, valueOffset, serialType);
			values.push(value);
			valueOffset = nextOffset;
		}
		return values;
	});

const decodeSchemaRow = (bytes: Uint8Array): Effect.Effect<SchemaRow, Error> =>
	decodeRecord(bytes).pipe(
		Effect.flatMap((values) =>
			values.length === 5 &&
			typeof values[0] === "string" &&
			typeof values[1] === "string" &&
			typeof values[2] === "string" &&
			typeof values[3] === "number" &&
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- sqlite_schema CDC can carry NULL SQL for implicit objects.
			(typeof values[4] === "string" || values[4] === null)
				? Effect.succeed({
						type: values[0],
						name: values[1],
						tblName: values[2],
						rootpage: values[3],
						sql: values[4],
					})
				: Effect.fail(new Error("Invalid sqlite_schema CDC record")),
		),
	);

const parseUpdatedColumns = (
	columns: readonly TableColumn[],
	values: readonly CDCValue[],
): Effect.Effect<readonly [readonly string[], readonly CDCValue[]], Error> => {
	if (values.length % 2 !== 0) {
		return Effect.fail(new Error("Invalid CDC updates record"));
	}
	const columnCount = values.length / 2;
	const recordColumns = columns.slice(0, columnCount);
	const changedColumns: string[] = [];
	const changedValues: CDCValue[] = [];
	for (let index = 0; index < columnCount; index += 1) {
		const flag = values[index];
		if (flag !== 0 && flag !== 1) {
			return Effect.fail(new Error(`Invalid CDC update flag at column ${index}`));
		}
		if (flag === 0) {
			continue;
		}
		if (index >= recordColumns.length) {
			return Effect.fail(new Error(`CDC updates reference missing column ${index}`));
		}
		changedColumns.push(recordColumns[index].name);
		changedValues.push(values[columnCount + index]);
	}
	return Effect.succeed([changedColumns, changedValues]);
};

const getTableColumns = (
	database: Database,
	tableName: string,
): Effect.Effect<readonly TableColumn[], Error> =>
	Effect.tryPromise(
		() =>
			database.all(`PRAGMA table_info("${tableName.replaceAll('"', '""')}")`) as Promise<
				readonly TableColumn[]
			>,
	).pipe(
		Effect.flatMap((columns) =>
			columns.length > 0
				? Effect.succeed(columns)
				: Effect.fail(new Error(`Table ${tableName} does not exist`)),
		),
		Effect.tapError((error) =>
			Effect.logError("cdc:get-table-columns").pipe(
				Effect.annotateLogs({ table: tableName, error: error.message }),
			),
		),
	);

const applySchemaInsert = (
	database: Database,
	change: Extract<CDCRow, { changeType: typeof CDCChangeType.Insert }>,
): Effect.Effect<void, Error> =>
	decodeSchemaRow(change.after).pipe(
		Effect.flatMap((row) => {
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- sqlite_schema rows may have NULL SQL and should be skipped.
			if (row.sql === null) {
				return Effect.void;
			}
			const sql = row.sql.replace(
				/^(CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|TRIGGER|VIEW|MATERIALIZED\s+VIEW)\s+)(?!IF\s+NOT\s+EXISTS\b)/i,
				"$1IF NOT EXISTS ",
			);
			return Effect.tryPromise({
				try: () => database.run(sql),
				catch: (error) =>
					new Error(`Failed to apply schema insert: ${sql}: ${String(error)}`),
			}).pipe(Effect.asVoid);
		}),
	);

const applySchemaDelete = (
	database: Database,
	change: Extract<CDCRow, { changeType: typeof CDCChangeType.Delete }>,
): Effect.Effect<void, Error> =>
	decodeSchemaRow(change.before).pipe(
		Effect.flatMap((row) => {
			// oxlint-disable-next-line local-rules/no-null-undefined-option -- sqlite_schema rows may have NULL SQL and should be skipped.
			if (row.sql === null || row.name.startsWith("sqlite_")) {
				return Effect.void;
			}
			const dropType =
				row.type === "table"
					? "TABLE"
					: row.type === "index"
						? "INDEX"
						: row.type === "view"
							? "VIEW"
							: row.type === "trigger"
								? "TRIGGER"
								: "";
			if (dropType === "") {
				return Effect.fail(new Error(`Unsupported sqlite_schema object type ${row.type}`));
			}
			const sql = `DROP ${dropType} IF EXISTS "${row.name.replaceAll('"', '""')}"`;
			return Effect.tryPromise({
				try: () => database.run(sql),
				catch: (error) =>
					new Error(`Failed to apply schema delete: ${sql}: ${String(error)}`),
			}).pipe(Effect.asVoid);
		}),
	);

const applySchemaUpdate = (
	database: Database,
	change: Extract<CDCRow, { changeType: typeof CDCChangeType.Update }>,
): Effect.Effect<void, Error> =>
	decodeRecord(change.updates).pipe(
		Effect.flatMap((values) => {
			if (values.length !== 10) {
				return Effect.fail(new Error("Invalid sqlite_schema CDC update record"));
			}
			const ddl = values[9];
			if (typeof ddl !== "string") {
				return Effect.fail(new Error("Invalid sqlite_schema DDL update statement"));
			}
			const match = ddl.match(
				/^ALTER\s+TABLE\s+("(?:[^"]|"")+"|`[^`]+`|\[[^\]]+\]|\S+)\s+ADD\s+COLUMN\s+("(?:[^"]|"")+"|`[^`]+`|\[[^\]]+\]|\S+)/i,
			);
			if (!match) {
				return Effect.tryPromise({
					try: () => database.run(ddl),
					catch: (error) =>
						new Error(`Failed to apply schema update: ${ddl}: ${String(error)}`),
				}).pipe(Effect.asVoid);
			}
			const tableName =
				match[1].startsWith('"') && match[1].endsWith('"')
					? match[1].slice(1, -1).replaceAll('""', '"')
					: match[1].startsWith("`") && match[1].endsWith("`")
						? match[1].slice(1, -1)
						: match[1].startsWith("[") && match[1].endsWith("]")
							? match[1].slice(1, -1)
							: match[1];
			const columnName =
				match[2].startsWith('"') && match[2].endsWith('"')
					? match[2].slice(1, -1).replaceAll('""', '"')
					: match[2].startsWith("`") && match[2].endsWith("`")
						? match[2].slice(1, -1)
						: match[2].startsWith("[") && match[2].endsWith("]")
							? match[2].slice(1, -1)
							: match[2];
			return getTableColumns(database, tableName).pipe(
				Effect.catchAll(() => Effect.succeed([] as const)),
				Effect.flatMap((columns) =>
					columns.some((column) => column.name === columnName)
						? Effect.void
						: Effect.tryPromise({
								try: () => database.run(ddl),
								catch: (error) =>
									new Error(
										`Failed to apply schema update: ${ddl}: ${String(error)}`,
									),
							}).pipe(Effect.asVoid),
				),
			);
		}),
	);

const applyInsert = (
	database: Database,
	change: Extract<CDCRow, { changeType: typeof CDCChangeType.Insert }>,
): Effect.Effect<void, Error> =>
	getTableColumns(database, change.tableName).pipe(
		Effect.flatMap((columns) =>
			decodeRecord(change.after).pipe(
				Effect.flatMap((values) => {
					const recordColumns = columns.slice(0, values.length);
					if (recordColumns.length !== values.length) {
						return Effect.fail(
							new Error(
								`CDC insert for ${change.tableName} has more columns than target table`,
							),
						);
					}
					const pkColumns = columns
						.filter((column) => column.pk > 0)
						.sort((left, right) => left.pk - right.pk);
					const columnSql = recordColumns
						.map((column) => `"${column.name.replaceAll('"', '""')}"`)
						.join(", ");
					const placeholders = recordColumns.map(() => "?").join(", ");
					const conflictSql =
						pkColumns.length === 0
							? ""
							: ` ON CONFLICT(${pkColumns.map((column) => `"${column.name.replaceAll('"', '""')}"`).join(", ")}) DO UPDATE SET ${recordColumns.map((column) => `"${column.name.replaceAll('"', '""')}" = excluded."${column.name.replaceAll('"', '""')}"`).join(", ")}`;
					const sql = `INSERT INTO "${change.tableName.replaceAll('"', '""')}" (${columnSql}) VALUES (${placeholders})${conflictSql}`;
					return Effect.tryPromise({
						try: () => database.run(sql, ...values),
						catch: (error) =>
							new Error(
								`Failed to apply insert to ${change.tableName}: ${String(error)}`,
							),
					}).pipe(Effect.asVoid);
				}),
			),
		),
	);

const applyDelete = (
	database: Database,
	change: Extract<CDCRow, { changeType: typeof CDCChangeType.Delete }>,
): Effect.Effect<void, Error> =>
	getTableColumns(database, change.tableName).pipe(
		Effect.flatMap((columns) =>
			decodeRecord(change.before).pipe(
				Effect.flatMap((values) => {
					const pkColumns = columns
						.filter((column) => column.pk > 0)
						.sort((left, right) => left.pk - right.pk);
					if (pkColumns.length === 0) {
						// oxlint-disable-next-line local-rules/no-null-undefined-option -- rowid-free deletes need a missing CDC id check.
						return change.id === null
							? Effect.fail(
									new Error(
										`Invalid CDC change ${change.changeId}: table ${change.tableName} has no primary key or rowid`,
									),
								)
							: Effect.tryPromise({
									try: () =>
										database.run(
											`DELETE FROM "${change.tableName.replaceAll('"', '""')}" WHERE rowid = ?`,
											change.id,
										),
									catch: (error) =>
										new Error(
											`Failed to apply delete by rowid on ${change.tableName}: ${String(error)}`,
										),
								}).pipe(Effect.asVoid);
					}
					const whereValues = pkColumns.map((column) => {
						const index = columns.findIndex((item) => item.name === column.name);
						if (index < 0 || index >= values.length) {
							throw new Error(
								`Missing primary key column ${column.name} in CDC delete for ${change.tableName}`,
							);
						}
						return values[index];
					});
					const sql = `DELETE FROM "${change.tableName.replaceAll('"', '""')}" WHERE ${pkColumns.map((column) => `"${column.name.replaceAll('"', '""')}" = ?`).join(" AND ")}`;
					return Effect.tryPromise({
						try: () => database.run(sql, ...whereValues),
						catch: (error) =>
							new Error(
								`Failed to apply delete on ${change.tableName}: ${String(error)}`,
							),
					}).pipe(Effect.asVoid);
				}),
			),
		),
	);

const applyUpdateWithMask = (
	database: Database,
	change: Extract<CDCRow, { changeType: typeof CDCChangeType.Update }>,
): Effect.Effect<void, Error> =>
	getTableColumns(database, change.tableName).pipe(
		Effect.flatMap((columns) =>
			Effect.all({
				after: decodeRecord(change.after),
				updates: decodeRecord(change.updates),
			}).pipe(
				Effect.flatMap(({ after, updates }) =>
					parseUpdatedColumns(columns, updates).pipe(
						Effect.flatMap(([changedColumns, changedValues]) => {
							if (changedColumns.length === 0) {
								return Effect.void;
							}
							const pkColumns = columns
								.filter((column) => column.pk > 0)
								.sort((left, right) => left.pk - right.pk);
							if (pkColumns.length === 0) {
								// oxlint-disable-next-line local-rules/no-null-undefined-option -- rowid-free updates need a missing CDC id check.
								return change.id === null
									? Effect.fail(
											new Error(
												`Invalid CDC change ${change.changeId}: table ${change.tableName} has no primary key or rowid`,
											),
										)
									: Effect.tryPromise({
											try: () =>
												database.run(
													`UPDATE "${change.tableName.replaceAll('"', '""')}" SET ${changedColumns.map((column) => `"${column.replaceAll('"', '""')}" = ?`).join(", ")} WHERE rowid = ?`,
													...changedValues,
													change.id,
												),
											catch: (error) =>
												new Error(
													`Failed to apply update by rowid on ${change.tableName}: ${String(error)}`,
												),
										}).pipe(Effect.asVoid);
							}
							const whereValues = pkColumns.map((column) => {
								const index = columns.findIndex(
									(item) => item.name === column.name,
								);
								if (index < 0 || index >= after.length) {
									throw new Error(
										`Missing primary key column ${column.name} in CDC update for ${change.tableName}`,
									);
								}
								return after[index];
							});
							const sql = `UPDATE "${change.tableName.replaceAll('"', '""')}" SET ${changedColumns.map((column) => `"${column.replaceAll('"', '""')}" = ?`).join(", ")} WHERE ${pkColumns.map((column) => `"${column.name.replaceAll('"', '""')}" = ?`).join(" AND ")}`;
							return Effect.tryPromise({
								try: () => database.run(sql, ...changedValues, ...whereValues),
								catch: (error) =>
									new Error(
										`Failed to apply update on ${change.tableName}: ${String(error)}`,
									),
							}).pipe(Effect.asVoid);
						}),
					),
				),
			),
		),
	);

const applyUpdateWithoutMask = (
	database: Database,
	change: Extract<CDCRow, { changeType: typeof CDCChangeType.Update }>,
): Effect.Effect<void, Error> =>
	applyDelete(database, {
		...change,
		changeType: CDCChangeType.Delete,
		// oxlint-disable-next-line local-rules/no-null-undefined-option -- update-as-delete replay must clear fields absent on delete CDC rows.
		after: null,
		// oxlint-disable-next-line local-rules/no-null-undefined-option -- update-as-delete replay must clear fields absent on delete CDC rows.
		updates: null,
	}).pipe(
		Effect.flatMap(() =>
			applyInsert(database, {
				...change,
				changeType: CDCChangeType.Insert,
				// oxlint-disable-next-line local-rules/no-null-undefined-option -- update-as-insert replay must clear fields absent on insert CDC rows.
				before: null,
				// oxlint-disable-next-line local-rules/no-null-undefined-option -- update-as-insert replay must clear fields absent on insert CDC rows.
				updates: null,
			}),
		),
	);

export const applyCDC = (database: Database, change: CDCRow): Effect.Effect<void, Error> => {
	switch (change.changeType) {
		case CDCChangeType.Insert:
			return change.tableName === "sqlite_schema"
				? applySchemaInsert(database, change)
				: applyInsert(database, change);
		case CDCChangeType.Delete:
			return change.tableName === "sqlite_schema"
				? applySchemaDelete(database, change)
				: applyDelete(database, change);
		case CDCChangeType.Update:
			return change.tableName === "sqlite_schema"
				? applySchemaUpdate(database, change)
				: // oxlint-disable-next-line local-rules/no-null-undefined-option -- updates without a mask are represented by NULL in the CDC stream.
					change.updates === null
					? applyUpdateWithoutMask(database, change)
					: applyUpdateWithMask(database, change);
	}
};

export const replayCDC = (
	database: Database,
	changes: readonly CDCRow[],
): Effect.Effect<void, ReplayError> => {
	if (changes.length === 0) {
		return Effect.void;
	}

	const replay = Effect.gen(function* () {
		yield* Effect.tryPromise({
			try: () => database.exec("BEGIN IMMEDIATE"),
			catch: (error) => new Error(`Failed to begin CDC transaction: ${String(error)}`),
		}).pipe(Effect.asVoid);

		const replayResult = yield* Effect.either(
			Effect.gen(function* () {
				for (const change of changes) {
					yield* applyCDC(database, change).pipe(
						Effect.annotateLogs({
							changeId: String(change.changeId),
							changeType: String(change.changeType),
							tableName: change.tableName,
						}),
						Effect.tapError((error) =>
							Effect.logError("cdc:error").pipe(
								Effect.annotateLogs({
									changeId: String(change.changeId),
									changeType: String(change.changeType),
									tableName: change.tableName,
									error: error.message,
								}),
							),
						),
					);
				}
			}).pipe(
				Effect.zipRight(
					Effect.tryPromise({
						try: () => database.exec("COMMIT"),
						catch: (error) =>
							new Error(`Failed to commit CDC transaction: ${String(error)}`),
					}).pipe(Effect.asVoid),
				),
			),
		);

		if (replayResult._tag === "Right") {
			return;
		}

		const error = replayResult.left;
		const rollbackResult = yield* Effect.either(
			Effect.tryPromise({
				try: () => database.exec("ROLLBACK"),
				catch: (rollbackError) =>
					new Error(`Failed to rollback CDC transaction: ${String(rollbackError)}`),
			}).pipe(Effect.asVoid),
		);

		if (rollbackResult._tag === "Left") {
			return yield* Effect.fail(
				new ReplayError({
					message: `Failed to replay CDC: ${error instanceof Error ? error.message : String(error)}. Rollback failed: ${rollbackResult.left.message}`,
					cause: error,
				}),
			);
		}

		return yield* Effect.fail(
			new ReplayError({
				message: `Failed to replay CDC: ${error instanceof Error ? error.message : String(error)}`,
				cause: error,
			}),
		);
	});

	return Effect.acquireUseRelease(
		Effect.tryPromise({
			try: () => database.exec("PRAGMA capture_data_changes_conn('off')"),
			catch: (error) => new Error(`Failed to disable CDC: ${String(error)}`),
		}).pipe(Effect.asVoid),
		() => replay,
		() =>
			Effect.tryPromise({
				try: () => database.exec("PRAGMA capture_data_changes_conn('full')"),
				catch: (error) => new Error(`Failed to enable CDC: ${String(error)}`),
			}).pipe(Effect.asVoid, Effect.orDie),
	).pipe(
		Effect.mapError((error) =>
			error instanceof ReplayError
				? error
				: new ReplayError({
						message: `Failed to replay CDC: ${error instanceof Error ? error.message : String(error)}`,
						cause: error,
					}),
		),
	);
};