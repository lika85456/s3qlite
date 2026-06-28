import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";

import type { CDCRow } from "./types";

const query = `
	SELECT
		change_id AS changeId,
		change_time AS changeTime,
		change_txn_id AS changeTxnId,
		change_type AS changeType,
		table_name AS tableName,
		id,
		before,
		after,
		updates
	FROM turso_cdc
	WHERE change_id > ?
		AND change_type != 2
	ORDER BY change_id ASC
`;

const latestChangeIdQuery = `
	SELECT COALESCE(MAX(change_id), 0) AS changeId
	FROM turso_cdc
`;

/**
 * Extracts CDC rows from the Turso database after a given change_id exclusively.
 */
export const extractCDC = (
	tursoConnection: Database,
	afterId: number,
): Effect.Effect<readonly CDCRow[]> =>
	Effect.tryPromise(() => tursoConnection.all(query, afterId) as Promise<CDCRow[]>).pipe(
		Effect.orDie,
		Effect.tap((r) =>
			Effect.logDebug(
				`extractCDC: extracted ${r.length} rows after change_id ${afterId}`,
			).pipe(Effect.orDie),
		),
	);

export const getLatestChangeId = (tursoConnection: Database): Effect.Effect<number, Error> =>
	Effect.tryPromise({
		try: async () => {
			const [row] = (await tursoConnection.all(latestChangeIdQuery)) as {
				// oxlint-disable-next-line local-rules/no-null-undefined-option -- SQLite aggregate MAX returns NULL when the CDC table is empty.
				changeId: number | null;
			}[];
			return row?.changeId ?? 0;
		},
		catch: (error) => new Error(`Failed to read latest CDC change id: ${String(error)}`),
	});
