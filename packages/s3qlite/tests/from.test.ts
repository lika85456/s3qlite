import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { FileSystem } from "@effect/platform/FileSystem";
import { expect } from "@effect/vitest";
import { connect as connectTurso } from "@tursodatabase/database";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { FileNotFoundError, fromInternal } from "../src/from";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";
import { AlreadyExistsError } from "../src/types";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

const withTempDir = (fs: FileSystem) =>
	Effect.gen(function* () {
		const tmpDir = `/tmp/s3qlite-from-test-${crypto.randomUUID()}`;
		yield* fs.makeDirectory(tmpDir, { recursive: true }).pipe(Effect.orDie);
		yield* Effect.addFinalizer(() =>
			fs.remove(tmpDir, { recursive: true, force: true }).pipe(Effect.orDie),
		);
		return tmpDir;
	});

rustfs("from", (it) => {
	it.effect("imports a local SQLite database and makes it accessible via connect", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `from-import-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("from-import");
				const importLocalKV = yield* makeMemoryKV();
				const verifyLocalKV = yield* makeMemoryKV();

				const fs = yield* FileSystem;
				const tmpDir = yield* withTempDir(fs);
				const tmpDbPath = `${tmpDir}/source.db`;

				{
					const db = yield* Effect.tryPromise(async () => {
						const c = await connectTurso(tmpDbPath);
						await c.exec(
							"CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)",
						);
						await c.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice");
						await c.run("PRAGMA wal_checkpoint(TRUNCATE)");
						return c;
					});
					yield* Effect.tryPromise(() => db.close());
				}

				yield* fromInternal(tmpDbPath, dbName).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, importLocalKV),
				);

				const imported = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, verifyLocalKV),
					),
					(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
				);

				const rows = yield* Effect.tryPromise(
					() =>
						imported.all("SELECT id, name FROM users ORDER BY id") as Promise<
							unknown[]
						>,
				);
				expect(rows).toEqual([{ id: "1", name: "alice" }]);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);

	it.effect("fails with AlreadyExistsError when target database already exists", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `from-exists-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("from-exists");
				const localKV = yield* makeMemoryKV();

				const first = yield* Effect.acquireRelease(
					connect(dbName, { bucket: "test" }).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKV),
					),
					(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
				);

				yield* Effect.tryPromise(() => first.exec("CREATE TABLE t (x INTEGER)"));
				yield* first.push();

				const fs = yield* FileSystem;
				const tmpDir = yield* withTempDir(fs);
				const tmpDbPath = `${tmpDir}/source.db`;

				{
					const db = yield* Effect.tryPromise(async () => {
						const c = await connectTurso(tmpDbPath);
						await c.exec("CREATE TABLE t (x INTEGER)");
						await c.run("PRAGMA wal_checkpoint(TRUNCATE)");
						return c;
					});
					yield* Effect.tryPromise(() => db.close());
				}

				const anotherLocalKV = yield* makeMemoryKV();

				const result = yield* Effect.either(
					fromInternal(tmpDbPath, dbName).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, anotherLocalKV),
					),
				);

				expect(result._tag).toBe("Left");
				if (result._tag === "Left") {
					expect(result.left).toBeInstanceOf(AlreadyExistsError);
				}
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);

	it.effect("fails with FileNotFoundError when source file does not exist", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `from-missing-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("from-missing");
				const localKV = yield* makeMemoryKV();

				const nonexistentPath = `/tmp/s3qlite-from-nonexistent-${crypto.randomUUID()}.db`;

				const result = yield* Effect.either(
					fromInternal(nonexistentPath, dbName).pipe(
						Effect.provideService(RemoteKV, remoteKV),
						Effect.provideService(LocalKV, localKV),
					),
				);

				expect(result._tag).toBe("Left");
				if (result._tag === "Left") {
					expect(result.left).toBeInstanceOf(FileNotFoundError);
					expect((result.left as FileNotFoundError).path).toBe(nonexistentPath);
				}
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});