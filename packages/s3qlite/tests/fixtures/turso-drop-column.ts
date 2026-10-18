/**
 * Child process for `turso-drop-column-panic.test.ts`. Turso aborts the whole process when
 * `ALTER TABLE ... DROP COLUMN` runs in a transaction while a statement with a correlated subquery
 * (prepared after an earlier ADD COLUMN in the same transaction) is still alive. s3qlite closes every
 * statement after use, so this must finish. Prints `OK <rows>` on success.
 */
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import { connect } from "../../src/connection";
import { makeMemoryKV } from "../../src/kv/memoryKV";
import * as S3qliteClient from "../../src/sql/client";
import { LocalKV, RemoteKV } from "../../src/storage";
import type { SyncMode } from "../../src/types";

const setup = [
	"CREATE TABLE account (id TEXT PRIMARY KEY, selected_org_id TEXT)",
	"CREATE TABLE account_state (id INTEGER PRIMARY KEY, active_account_id TEXT)",
	"INSERT INTO account VALUES ('a1', 'o1')",
	"INSERT INTO account_state VALUES (1, 'a1')",
];
const migration = [
	"ALTER TABLE account_state ADD active_org_id TEXT",
	"UPDATE account_state SET active_org_id = (SELECT selected_org_id FROM account WHERE account.id = account_state.active_account_id)",
	"ALTER TABLE account DROP COLUMN selected_org_id",
];

const [frontEnd = "sql", mode = "manual"] = process.argv.slice(2);
const syncMode: SyncMode = mode === "owner" ? { type: "owner" } : "manual";

const kv = Layer.merge(
	Layer.effect(RemoteKV, makeMemoryKV()),
	Layer.effect(LocalKV, makeMemoryKV()),
).pipe(Layer.provide(fileSystemLayer));

const viaSqlClient = Effect.gen(function* () {
	const sql = yield* SqlClient;
	for (const statement of setup) {
		yield* sql.unsafe(statement);
	}
	yield* sql.withTransaction(Effect.forEach(migration, (statement) => sql.unsafe(statement)));
	return yield* sql.unsafe("SELECT id, active_account_id, active_org_id FROM account_state");
}).pipe(
	Effect.provide(
		S3qliteClient.layerFromKV({ dbName: "panic", bucket: "test", syncMode }).pipe(
			Layer.provide(kv),
		),
	),
);

// The exact SQL of the bug report, BEGIN and COMMIT included, through the Database wrapper.
const viaDatabase = Effect.gen(function* () {
	const database = yield* connect("panic", { bucket: "test", syncMode });
	for (const statement of [...setup, "BEGIN", ...migration, "COMMIT"]) {
		yield* Effect.promise(() => database.exec(statement));
	}
	return yield* Effect.promise(() =>
		database.all("SELECT id, active_account_id, active_org_id FROM account_state"),
	);
}).pipe(Effect.scoped, Effect.provide(kv));

const run: Effect.Effect<unknown, unknown> = frontEnd === "sql" ? viaSqlClient : viaDatabase;
const rows = await Effect.runPromise(run);
console.log(`OK ${JSON.stringify(rows)}`);