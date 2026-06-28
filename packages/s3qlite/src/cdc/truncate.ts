import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";

export const truncate = (db: Database) =>
	Effect.promise(() => db.run("PRAGMA wal_checkpoint(TRUNCATE);"));
