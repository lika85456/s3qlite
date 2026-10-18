import { Context, Effect, Layer, Option } from "effect";
import * as SqlClient from "effect/sql/SqlClient";

import { ConflictError } from "../kv/kv";
import type { KV } from "../kv/kv";
import { decodeJson, encodeJson, getJson, headKey } from "../storage";
import type { Head } from "../types";

export type VersionedHead = {
	readonly head: Head;
	/** Opaque version token: the S3 ETag, or the generation of an external store. */
	readonly version: string;
};

/**
 * Where owner mode keeps the authoritative head and performs the fencing compare-and-set.
 *
 * The default stores `{dbName}.json` in the bucket and uses S3 conditional writes
 * (`If-None-Match: *` / `If-Match`). That is only safe when conditional writes on one key are
 * linearizable, which holds for AWS S3 and single-endpoint deployments, but not for every
 * S3-compatible store behind several endpoints (see RustFS issue #1659). An external store such as
 * Postgres (`HeadStore.layerSql`) moves the compare-and-set out of the object store; objects still
 * live in the bucket and are never overwritten.
 */
export type HeadStoreShape = {
	/** `true` when the head lives outside the bucket; the owner then mirrors it to `{dbName}.json`. */
	readonly external: boolean;
	readonly read: (dbName: string) => Effect.Effect<Option.Option<VersionedHead>>;
	/** Creates the head; fails with `ConflictError` when it exists. */
	readonly create: (dbName: string, head: Head) => Effect.Effect<string, ConflictError>;
	/** Replaces the head when its version is still `version`; fails with `ConflictError` otherwise. */
	readonly cas: (
		dbName: string,
		head: Head,
		version: string,
	) => Effect.Effect<string, ConflictError>;
};

export class HeadStore extends Context.Service<HeadStore, HeadStoreShape>()(
	"@lika85456/s3qlite/HeadStore",
) {
	/** Head store in a SQL database (for example Postgres); see `makeSql`. */
	static readonly layerSql = (
		options: { readonly table?: string } = {},
	): Layer.Layer<HeadStore, Error, SqlClient.SqlClient> =>
		Layer.effect(HeadStore, makeSql(options));
	static readonly makeSql = (
		options: { readonly table?: string } = {},
	): Effect.Effect<HeadStoreShape, Error, SqlClient.SqlClient> => makeSql(options);
	/** The default store: `{dbName}.json` in the bucket. */
	static readonly remote = (remote: KV): HeadStoreShape => remoteHeadStore(remote);
}

/** The default head store: `{dbName}.json` in the bucket, fenced with S3 conditional writes. */
export const remoteHeadStore = (remote: KV): HeadStoreShape => ({
	external: false,
	read: (dbName) =>
		getJson<Head>(remote, headKey(dbName)).pipe(
			Effect.orDie,
			Effect.map(Option.map(({ etag, value }) => ({ head: value, version: etag }))),
		),
	create: (dbName, head) =>
		remote.create(headKey(dbName), encodeJson(head)).pipe(Effect.map(({ etag }) => etag)),
	cas: (dbName, head, version) =>
		remote.cas(headKey(dbName), encodeJson(head), version).pipe(Effect.map(({ etag }) => etag)),
});

const validTableName = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Head store in a SQL database (tested with Postgres through `@effect/sql-pg`). The version is a
 * generation counter, and the compare-and-set is one `UPDATE ... WHERE generation = $expected`.
 * The table is created when the layer is built.
 */
export const makeSql = (
	options: { readonly table?: string } = {},
): Effect.Effect<HeadStoreShape, Error, SqlClient.SqlClient> =>
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient;
		const tableName = options.table ?? "s3qlite_heads";
		if (!validTableName.test(tableName)) {
			return yield* Effect.fail(new Error(`Invalid head store table name "${tableName}"`));
		}
		const table = sql(tableName);
		yield* sql`CREATE TABLE IF NOT EXISTS ${table} (
			db_name TEXT PRIMARY KEY,
			generation BIGINT NOT NULL,
			head TEXT NOT NULL
		)`.pipe(
			Effect.mapError(
				(error) => new Error(`Failed to create ${tableName}: ${error.message}`),
			),
		);

		type Row = { readonly generation: number | string; readonly head: string };

		const textDecoder = new TextDecoder();
		const textEncoder = new TextEncoder();

		return {
			external: true,
			read: (dbName) =>
				sql<Row>`SELECT generation, head FROM ${table} WHERE db_name = ${dbName}`.pipe(
					Effect.orDie,
					Effect.flatMap((rows) =>
						Option.match(Option.fromNullishOr(rows[0]), {
							onNone: () => Effect.succeed(Option.none<VersionedHead>()),
							onSome: (row) =>
								decodeJson<Head>(textEncoder.encode(row.head)).pipe(
									Effect.orDie,
									Effect.map((head) =>
										Option.some({ head, version: String(row.generation) }),
									),
								),
						}),
					),
				),
			create: (dbName, head) =>
				sql<{
					readonly generation: number | string;
				}>`INSERT INTO ${table} (db_name, generation, head)
					VALUES (${dbName}, 1, ${textDecoder.decode(encodeJson(head))})
					ON CONFLICT (db_name) DO NOTHING
					RETURNING generation`.pipe(
					Effect.orDie,
					Effect.flatMap((rows) =>
						rows.length === 1
							? Effect.succeed("1")
							: Effect.fail(new ConflictError({ key: dbName })),
					),
				),
			cas: (dbName, head, version) =>
				sql<{ readonly generation: number | string }>`UPDATE ${table}
					SET generation = generation + 1, head = ${textDecoder.decode(encodeJson(head))}
					WHERE db_name = ${dbName} AND generation = ${Number(version)}
					RETURNING generation`.pipe(
					Effect.orDie,
					Effect.flatMap((rows) =>
						Option.match(Option.fromNullishOr(rows[0]), {
							onNone: () => Effect.fail(new ConflictError({ key: dbName })),
							onSome: (row) => Effect.succeed(String(row.generation)),
						}),
					),
				),
		} satisfies HeadStoreShape;
	});