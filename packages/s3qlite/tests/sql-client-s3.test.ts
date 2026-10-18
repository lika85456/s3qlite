import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect, Exit, Layer, Scope } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import * as S3qliteClient from "../src/sql/client";

import { Rustfs, rustfs } from "./utils/rustfs";
import { testTmpPath } from "./utils/tmp";

rustfs("S3qliteClient.layer over S3", (it) => {
	it.effect("syncs two SqlClient layers through a real bucket", () =>
		Effect.gen(function* () {
			const s3 = yield* S3;
			const { makeBucketName, makeDbName } = yield* Rustfs;
			const bucket = makeBucketName("sql");
			const dbName = makeDbName("sql");
			yield* s3.createBucket({ Bucket: bucket });

			const config = { dbName, bucket };
			const directory = testTmpPath(`s3qlite-sql-${crypto.randomUUID()}`);

			// The writer layer pushes its unpushed changes when its scope closes.
			const writerScope = yield* Scope.make();
			const writer = yield* Layer.buildWithScope(
				S3qliteClient.layer({
					...config,
					localDirectory: `${directory}/writer`,
					pushOnClose: true,
				}).pipe(Layer.provide(fileSystemLayer)),
				writerScope,
			);
			yield* Effect.gen(function* () {
				const sql = yield* SqlClient;
				const s3qlite = yield* S3qliteClient.S3qlite;
				yield* sql`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
				yield* sql.withTransaction(sql`INSERT INTO notes VALUES (1, 'pushed explicitly')`);
				yield* s3qlite.push;
				yield* sql`INSERT INTO notes VALUES (2, 'pushed on close')`;
			}).pipe(Effect.provideContext(writer));
			yield* Scope.close(writerScope, Exit.void);

			const rows = yield* Effect.gen(function* () {
				const sql = yield* SqlClient;
				return yield* sql`SELECT id, body FROM notes ORDER BY id`;
			}).pipe(
				Effect.provide(
					S3qliteClient.layer({ ...config, localDirectory: `${directory}/reader` }).pipe(
						Layer.provide(fileSystemLayer),
					),
				),
			);

			expect(rows).toEqual([
				{ id: 1, body: "pushed explicitly" },
				{ id: 2, body: "pushed on close" },
			]);
			yield* Effect.promise(() =>
				import("node:fs/promises").then(({ rm }) =>
					rm(directory, { recursive: true, force: true }),
				),
			);
		}),
	);
});