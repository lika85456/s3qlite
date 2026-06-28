import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect } from "effect";

import { BucketDoesNotExistError, connect } from "../effect";
import { connect as connectPromise } from "../index";

import { Rustfs, rustfs } from "./utils/rustfs";

rustfs("effect entrypoint", (it) => {
	it.effect("uses the raw scoped effect api with explicit dependencies", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const s3 = yield* S3;
				const rustfsState = yield* Rustfs;
				const bucket = rustfsState.makeBucketName("effect");
				const dbName = rustfsState.makeDbName("effect");
				const localDirectory = `/tmp/opencode/${dbName}`;

				yield* s3.createBucket({ Bucket: bucket });

				const db = yield* Effect.acquireRelease(
					connect(dbName, { bucket, localDirectory }).pipe(
						Effect.provide(fileSystemLayer),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					db.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
				);
				yield* Effect.tryPromise(() =>
					db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
				);
				yield* db.push();

				const users = yield* Effect.tryPromise(
					() => db.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				);

				expect(users).toEqual([{ id: "1", name: "alice" }]);
			}),
		),
	);

	it.effect("fails with BucketDoesNotExistError when the bucket is missing", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const rustfsState = yield* Rustfs;
				const bucket = rustfsState.makeBucketName("missing");
				const dbName = rustfsState.makeDbName("missing");
				const localDirectory = `/tmp/opencode/${dbName}`;

				const result = yield* Effect.either(
					connect(dbName, { bucket, localDirectory }).pipe(
						Effect.provide(fileSystemLayer),
					),
				);

				expect(result._tag).toBe("Left");
				if (result._tag === "Left") {
					expect(result.left).toBeInstanceOf(BucketDoesNotExistError);
					if (result.left instanceof BucketDoesNotExistError) {
						expect(result.left._tag).toBe("BucketDoesNotExistError");
						expect(result.left.bucket).toBe(bucket);
						expect(result.left.message).toBe(`Bucket "${bucket}" does not exist`);
					}
				}
			}),
		),
	);

	it.effect("promise api provides platform and keeps the connection open until close", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const s3 = yield* S3;
				const rustfsState = yield* Rustfs;
				const bucket = rustfsState.makeBucketName("promise");
				const dbName = rustfsState.makeDbName("promise");

				yield* s3.createBucket({ Bucket: bucket });

				const db = yield* Effect.acquireRelease(
					Effect.tryPromise(() =>
						connectPromise(dbName, {
							bucket,
							localDirectory: `/tmp/opencode/${dbName}`,
							s3: {
								credentials: {
									accessKeyId: rustfsState.accessKey,
									secretAccessKey: rustfsState.secretKey,
								},
								endpoint: rustfsState.endpoint,
								forcePathStyle: true,
								region: rustfsState.region,
							},
						}),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					db.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
				);
				yield* Effect.tryPromise(() =>
					db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
				);
				yield* Effect.tryPromise(() => db.push());

				const users = yield* Effect.tryPromise(
					() => db.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				);

				expect(users).toEqual([{ id: "1", name: "alice" }]);
			}),
		),
	);
});