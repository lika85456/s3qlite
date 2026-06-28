import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect, Option } from "effect";

import { connect } from "../src/connection";
import { ConnectionConfig } from "../src/contexts";
import {
	AlreadyExistsError,
	SameNameError,
	SourceDoesNotExistError,
	fork as runFork,
} from "../src/fork";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, getJson, headKey } from "../src/storage";
import type { Head } from "../src/types";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

const getRemoteHead = (dbName: string, kv: RemoteKV["Type"]) =>
	getJson<Head>(kv, headKey(dbName)).pipe(
		Effect.flatMap(
			Option.match({
				onNone: () => Effect.die("missing remote head"),
				onSome: (head) => Effect.succeed(head),
			}),
		),
	);

rustfs("fork", (it) => {
	it.effect("copies the current head to a new database name and lets both heads diverge", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const sourceDbName = `fork-source-${crypto.randomUUID().slice(0, 8)}`;
				const forkDbName = `fork-target-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("fork");
				const sourceLocalKV = yield* makeMemoryKV();
				const forkLocalKV = yield* makeMemoryKV();
				const sourceVerifyLocalKV = yield* makeMemoryKV();
				const forkVerifyLocalKV = yield* makeMemoryKV();

				const source = yield* Effect.acquireRelease(
					connect(sourceDbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, sourceLocalKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					source.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
				);
				yield* Effect.tryPromise(() =>
					source.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
				);
				yield* source.push();

				const sourceHeadBeforeFork = yield* getRemoteHead(sourceDbName, remoteKV);

				yield* source.fork(forkDbName);

				const forkHeadAfterFork = yield* getRemoteHead(forkDbName, remoteKV);
				expect(forkHeadAfterFork.value).toEqual(sourceHeadBeforeFork.value);

				const forked = yield* Effect.acquireRelease(
					connect(forkDbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, forkLocalKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				const forkRowsBeforePush = yield* Effect.tryPromise(
					() =>
						forked.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				);
				expect(forkRowsBeforePush).toEqual([{ id: "1", name: "alice" }]);

				yield* Effect.tryPromise(() =>
					forked.run("INSERT INTO users (id, name) VALUES (?, ?)", "2", "bob"),
				);
				yield* forked.push();

				const sourceVerifier = yield* Effect.acquireRelease(
					connect(sourceDbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, sourceVerifyLocalKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				const forkVerifier = yield* Effect.acquireRelease(
					connect(forkDbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, forkVerifyLocalKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				const sourceRows = yield* Effect.tryPromise(
					() =>
						sourceVerifier.all("SELECT id, name FROM users ORDER BY id") as Promise<
							unknown[]
						>,
				);
				const forkRows = yield* Effect.tryPromise(
					() =>
						forkVerifier.all("SELECT id, name FROM users ORDER BY id") as Promise<
							unknown[]
						>,
				);

				expect(sourceRows).toEqual([{ id: "1", name: "alice" }]);
				expect(forkRows).toEqual([
					{ id: "1", name: "alice" },
					{ id: "2", name: "bob" },
				]);

				const sourceHeadAfterForkPush = yield* getRemoteHead(sourceDbName, remoteKV);
				const forkHeadAfterForkPush = yield* getRemoteHead(forkDbName, remoteKV);

				expect(sourceHeadAfterForkPush.value.batches).toHaveLength(1);
				expect(forkHeadAfterForkPush.value.batches).toHaveLength(2);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);

	it.effect("fails with SameNameError when the fork name matches the source name", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `fork-same-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("fork-same");
				const localKV = yield* makeMemoryKV();

				const db = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				const result = yield* Effect.either(db.fork(dbName));
				expect(result._tag).toBe("Left");
				if (result._tag === "Left") {
					expect(result.left).toBeInstanceOf(SameNameError);
					expect(result.left._tag).toBe("SameNameError");
					expect(result.left.dbName).toBe(dbName);
				}
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);

	it.effect("fails with AlreadyExistsError when the target database already exists", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const sourceDbName = `fork-exists-source-${crypto.randomUUID().slice(0, 8)}`;
				const targetDbName = `fork-exists-target-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("fork-exists");
				const sourceLocalKV = yield* makeMemoryKV();
				const targetLocalKV = yield* makeMemoryKV();

				const source = yield* Effect.acquireRelease(
					connect(sourceDbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, sourceLocalKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				const target = yield* Effect.acquireRelease(
					connect(targetDbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, targetLocalKV),
					),
					(instance) => Effect.tryPromise(() => instance.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() =>
					source.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
				);
				yield* source.push();
				yield* target.push();

				const result = yield* Effect.either(source.fork(targetDbName));
				expect(result._tag).toBe("Left");
				if (result._tag === "Left") {
					expect(result.left).toBeInstanceOf(AlreadyExistsError);
					expect(result.left._tag).toBe("AlreadyExistsError");
					expect(result.left.dbName).toBe(targetDbName);
				}
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);

	it.effect("fails with SourceDoesNotExistError when the source head is missing", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `fork-missing-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("fork-missing");
				const targetDbName = `fork-target-${crypto.randomUUID().slice(0, 8)}`;
				const result = yield* Effect.either(
					runFork(targetDbName).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(ConnectionConfig, {
							bucket: "test",
							dbName,
							livePath: `${dbName}.db`,
							localDirectory: "./.s3qlite/",
							localHeadPath: `${dbName}.json`,
							options: { bucket: "test" },
						}),
					),
				);

				expect(result._tag).toBe("Left");
				if (result._tag === "Left") {
					expect(result.left).toBeInstanceOf(SourceDoesNotExistError);
					expect(result.left._tag).toBe("SourceDoesNotExistError");
					expect(result.left.dbName).toBe(dbName);
				}
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});