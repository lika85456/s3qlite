// Copied from ~/workspace/Personal/sites/packages/web-fetch/src/platform/cluster/pg-types.ts
// (regclass workaround). @effect/sql-pg 4.0.5 ships a regclass codec, older 4.0.0 release
// candidates do not; the explicit codec keeps the runners working on both.
import * as PgTypes from "@effect/sql-pg/PgTypes";
import * as Result from "effect/Result";

const REGCLASS_OID = 2205;

/**
 * effect's SQL Migrator, used by the cluster message storage, checks for its table with
 * `select 'cluster_migrations'::regclass`. Without a regclass codec the binary OID is decoded as
 * UTF-8 text, which fails on every start after the first. A regclass value is an unsigned 32-bit OID.
 */
export const clusterPgTypes: PgTypes.Registry = PgTypes.makeRegistry();

clusterPgTypes.register<number>(REGCLASS_OID, {
	encode: (value) => {
		const bytes = new Uint8Array(4);
		new DataView(bytes.buffer).setUint32(0, value);
		return Result.succeed(bytes);
	},
	decode: (bytes) =>
		bytes.byteLength === 4
			? Result.succeed(new DataView(bytes.buffer, bytes.byteOffset, 4).getUint32(0))
			: Result.fail(
					new PgTypes.CodecError({
						message: `regclass expects 4 bytes, got ${bytes.byteLength}`,
					}),
				),
});