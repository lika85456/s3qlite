import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";

export type SchemaEntry = {
	type: string;
	name: string;
	tblName: string;
	// oxlint-disable-next-line local-rules/no-null-undefined-option -- sqlite_schema.sql is NULL for implicit indexes and similar internal objects.
	sql: string | null;
};

export type DatabaseSnapshot = {
	schema: readonly SchemaEntry[];
	tables: Readonly<Record<string, readonly Record<string, unknown>[]>>;
};

export const compareSnapshots = (left: DatabaseSnapshot, right: DatabaseSnapshot): boolean =>
	JSON.stringify(left) === JSON.stringify(right);

export const snapshotDatabase = (database: Database): Effect.Effect<DatabaseSnapshot, Error> =>
	Effect.gen(function* () {
		const schema = yield* Effect.tryPromise(
			() =>
				database.all(
					[
						"SELECT type, name, tbl_name AS tblName, sql",
						"FROM sqlite_schema",
						"WHERE name NOT LIKE 'sqlite_%' AND name NOT LIKE 'turso_%'",
						"ORDER BY type, name",
					].join(" "),
				) as Promise<readonly SchemaEntry[]>,
		);
		const tableSnapshots = yield* Effect.forEach(
			schema.filter((entry) => entry.type === "table"),
			(entry) =>
				Effect.gen(function* () {
					const rows = yield* Effect.tryPromise(
						() =>
							database.all(
								`SELECT * FROM "${entry.name.replaceAll('"', '""')}"`,
							) as Promise<readonly Record<string, unknown>[]>,
					);
					const normalize = (row: Record<string, unknown>): string =>
						JSON.stringify(
							Object.fromEntries(
								Object.entries(row).map(([key, value]) => [
									key,
									value instanceof Uint8Array
										? { blob: Array.from(value) }
										: value,
								]),
							),
						);
					return [
						entry.name,
						[...rows].sort((left, right) =>
							normalize(left).localeCompare(normalize(right)),
						),
					] as const;
				}),
			{ concurrency: 1 },
		);
		return {
			schema: schema.map((entry) => ({
				...entry,
				sql: entry.sql
					? entry.sql.replace(
							/^(CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|TRIGGER|VIEW|MATERIALIZED\s+VIEW)\s+)IF\s+NOT\s+EXISTS\s+/i,
							"$1",
						)
					: // oxlint-disable-next-line local-rules/no-null-undefined-option -- snapshotting must preserve sqlite_schema NULL SQL entries.
						null,
			})),
			tables: Object.fromEntries(tableSnapshots),
		};
	});

export const compareDatabases = (left: Database, right: Database): Effect.Effect<boolean, Error> =>
	Effect.all([snapshotDatabase(left), snapshotDatabase(right)], { concurrency: 1 }).pipe(
		Effect.map(([leftSnapshot, rightSnapshot]) =>
			compareSnapshots(leftSnapshot, rightSnapshot),
		),
	);

export const applyBatch = (
	database: Database,
	batch: {
		readonly transactional: boolean;
		readonly statements: readonly string[];
	},
): Effect.Effect<void, Error> =>
	batch.transactional
		? Effect.gen(function* () {
				yield* Effect.tryPromise(() => database.exec("BEGIN IMMEDIATE")).pipe(
					Effect.asVoid,
				);
				yield* Effect.forEach(
					batch.statements,
					(statement) =>
						Effect.tryPromise(() => database.exec(statement)).pipe(Effect.asVoid),
					{ concurrency: 1, discard: true },
				);
				yield* Effect.tryPromise(() => database.exec("COMMIT")).pipe(Effect.asVoid);
			}).pipe(
				Effect.catchAll((error) =>
					Effect.tryPromise(() => database.exec("ROLLBACK")).pipe(
						Effect.asVoid,
						Effect.catchAll(() => Effect.void),
						Effect.andThen(Effect.fail(error)),
					),
				),
			)
		: Effect.forEach(
				batch.statements,
				(statement) =>
					Effect.tryPromise(() => database.exec(statement)).pipe(Effect.asVoid),
				{ concurrency: 1, discard: true },
			);