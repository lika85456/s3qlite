import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";

export const withoutCDC = <A, E, R>(
	database: Database,
	effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
	Effect.acquireUseRelease(
		Effect.promise(() => database.exec("PRAGMA capture_data_changes_conn('off')")),
		() => effect,
		() => Effect.promise(() => database.exec("PRAGMA capture_data_changes_conn('full')")),
	);