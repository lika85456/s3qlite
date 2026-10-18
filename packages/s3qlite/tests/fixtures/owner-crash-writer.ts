/**
 * Child process for `owner-crash-recovery.test.ts`: an owner that writes rows `1, 2, ...` and prints
 * `ACK <n>` after each write returns. The "remote" is a directory shared with the parent (a file KV
 * with a little latency per write), so the parent can SIGKILL this process in the middle of a push.
 *
 * Arguments: <remoteDir> <localDir> <dbName> <durable|fast>
 */
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import { makeFileKV } from "../../src/kv/fileKV";
import * as S3qliteClient from "../../src/sql/client";
import { LocalKV, RemoteKV } from "../../src/storage";

const [remoteDir, localDir, dbName, reply] = process.argv.slice(2);

const slowRemote = makeFileKV(remoteDir).pipe(
	Effect.map((kv) => ({
		...kv,
		set: (key: string, value: Uint8Array) =>
			Effect.sleep(3).pipe(Effect.andThen(kv.set(key, value))),
		cas: (key: string, value: Uint8Array, etag: string) =>
			Effect.sleep(3).pipe(Effect.andThen(kv.cas(key, value, etag))),
	})),
);

const program = Effect.gen(function* () {
	const sql = yield* SqlClient;
	yield* sql`CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY)`;
	const [last] = yield* sql<{ readonly id: number }>`SELECT COALESCE(MAX(id), 0) AS id FROM rows`;
	for (let id = last.id + 1; ; id += 1) {
		yield* sql`INSERT INTO rows (id) VALUES (${id})`;
		console.log(`ACK ${id}`);
	}
});

await Effect.runPromise(
	program.pipe(
		Effect.provide(
			S3qliteClient.layerFromKV({
				dbName,
				bucket: "test",
				syncMode: {
					type: "owner",
					reply: reply === "fast" ? "fast" : "durable",
					pushDelayMs: 5,
				},
			}).pipe(
				Layer.provide(Layer.effect(RemoteKV, slowRemote)),
				Layer.provide(Layer.effect(LocalKV, makeFileKV(localDir))),
				Layer.provide(fileSystemLayer),
			),
		),
	),
);