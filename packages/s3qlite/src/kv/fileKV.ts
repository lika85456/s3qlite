import { connect as connectTurso } from "@tursodatabase/database";
import type { Database } from "@tursodatabase/database";
import type { Scope } from "effect";
import { Effect, Option, Stream } from "effect";
import { FileSystem } from "effect/FileSystem";
import { constants } from "node:fs";
import { copyFile as copyFilePromise, open, readdir, rm, stat } from "node:fs/promises";
import { join, relative } from "node:path";

import { ConflictError } from "./kv";
import type { CloneTrait, ConnectDatabaseTrait, KV, ListedObject } from "./kv";

// TODO: in the future the etag should be probably cached
const getEtag = (value: Uint8Array): Effect.Effect<string> =>
	Effect.promise(async () => {
		const digest = await crypto.subtle.digest("SHA-256", Uint8Array.from(value).buffer);
		return Array.from(new Uint8Array(digest), (byte) =>
			byte.toString(16).padStart(2, "0"),
		).join("");
	});

const errorCode = (error: unknown): Option.Option<string> =>
	typeof error === "object" &&
	Option.isSome(Option.fromNullishOr(error)) &&
	"code" in (error as object) &&
	typeof (error as { code: unknown }).code === "string"
		? Option.some((error as { code: string }).code)
		: Option.none();

const lockStaleMs = 10_000;
const lockTimeoutMs = 30_000;

/**
 * Cross-process mutual exclusion for read-check-write sequences (`cas`). The lock is a sibling
 * `.lock` file created with `O_EXCL`; a lock older than `lockStaleMs` is assumed to belong to a
 * crashed writer and is broken.
 */
const acquireFileLock = async (lockPath: string): Promise<void> => {
	const startedAt = Date.now();

	while (true) {
		const handle = await open(lockPath, "wx").then(
			(file) => Option.some(file),
			(error: unknown) => {
				if (Option.getOrElse(errorCode(error), () => "") !== "EEXIST") {
					throw error;
				}
				return Option.none<Awaited<ReturnType<typeof open>>>();
			},
		);

		if (Option.isSome(handle)) {
			await handle.value.close();
			return;
		}

		const modifiedAt = await stat(lockPath).then(
			(value) => Option.some(value.mtimeMs),
			() => Option.none<number>(),
		);
		if (Option.exists(modifiedAt, (mtimeMs) => Date.now() - mtimeMs > lockStaleMs)) {
			await rm(lockPath, { force: true });
			continue;
		}

		if (Date.now() - startedAt > lockTimeoutMs) {
			throw new Error(`Timed out waiting for file lock ${lockPath}`);
		}

		await new Promise((resolve) => setTimeout(resolve, 2));
	}
};

export const makeFileKV = (
	rootDirectory: string,
): Effect.Effect<KV & CloneTrait & ConnectDatabaseTrait, never, FileSystem | Scope.Scope> =>
	Effect.gen(function* () {
		const fs = yield* FileSystem;
		yield* fs.makeDirectory(rootDirectory, { recursive: true }).pipe(Effect.orDie);

		/**
		 * Resolves `key` to a path, creates its parent directory and picks a unique sibling temp
		 * path, so the final file can be published with an atomic rename or link.
		 */
		const prepareWrite = (key: string) =>
			Effect.gen(function* () {
				const path = `${rootDirectory}/${key}`;
				const directory = path.split("/").slice(0, -1).join("/");
				yield* fs.makeDirectory(directory, { recursive: true }).pipe(Effect.orDie);
				return { path, tempPath: `${path}.${crypto.randomUUID()}.tmp` };
			});

		const writeBytes = (key: string, value: Uint8Array): Effect.Effect<{ etag: string }> =>
			Effect.gen(function* () {
				const { path, tempPath } = yield* prepareWrite(key);
				yield* fs.writeFile(tempPath, value).pipe(Effect.orDie);
				yield* fs.rename(tempPath, path).pipe(Effect.orDie);
				return { etag: yield* getEtag(value) };
			});

		const withKeyLock = <A, E>(key: string, effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
			Effect.acquireUseRelease(
				Effect.gen(function* () {
					const lockPath = (yield* prepareWrite(`${key}.lock`)).path;
					yield* Effect.promise(() => acquireFileLock(lockPath));
					return lockPath;
				}),
				() => effect,
				(lockPath) => Effect.promise(() => rm(lockPath, { force: true })),
			);

		const readCurrent = (key: string) =>
			Effect.gen(function* () {
				const path = `${rootDirectory}/${key}`;
				if (!(yield* fs.exists(path).pipe(Effect.orDie))) {
					return Option.none<{ value: Uint8Array; etag: string }>();
				}

				const value = yield* fs.readFile(path).pipe(Effect.orDie);
				return Option.some({ etag: yield* getEtag(value), value });
			});

		const openDatabase = (key: string): Effect.Effect<Database, Error> =>
			Effect.tryPromise({
				try: async () => {
					const connection = await connectTurso(`${rootDirectory}/${key}`);
					await connection.exec("PRAGMA capture_data_changes_conn('full')");
					return connection;
				},
				catch: (e) =>
					new Error(`Failed to connect to database at key ${key}: ${String(e)}`),
			}).pipe(
				// Database names may contain "/" (for example `<entityType>/<entityId>`).
				(open) => prepareWrite(key).pipe(Effect.andThen(open)),
				Effect.tap(() => Effect.logDebug(`fileKV.open(${key}) => database`)),
			);

		return {
			get: (key) =>
				readCurrent(key).pipe(
					Effect.tap((result) =>
						Effect.logDebug(
							`fileKV.get(${key}) => ${Option.match(result, {
								onNone: () => "None",
								onSome: ({ etag }) => `Some(${etag})`,
							})}`,
						),
					),
				),
			getIfChanged: (key, etag) =>
				readCurrent(key).pipe(
					Effect.map(Option.filter((current) => current.etag !== etag)),
					Effect.tap((result) =>
						Effect.logDebug(
							`fileKV.getIfChanged(${key}, ${etag}) => ${Option.isSome(result) ? "changed" : "None"}`,
						),
					),
				),
			exists: (key) =>
				fs.exists(`${rootDirectory}/${key}`).pipe(
					Effect.orDie,
					Effect.tap((result) => Effect.logDebug(`fileKV.exists(${key}) => ${result}`)),
				),
			set: (key, value) =>
				writeBytes(key, value).pipe(
					Effect.tap((result) => Effect.logDebug(`fileKV.set(${key}) => ${result.etag}`)),
				),
			cas: (key, value, etag) =>
				withKeyLock(
					key,
					Effect.gen(function* () {
						const current = yield* readCurrent(key);
						if (Option.isNone(current) || current.value.etag !== etag) {
							yield* Effect.logDebug(`fileKV.cas(${key}, ${etag}) => ConflictError`);
							return yield* new ConflictError({ key });
						}

						const result = yield* writeBytes(key, value);
						yield* Effect.logDebug(`fileKV.cas(${key}) => ${result.etag}`);
						return result;
					}),
				),
			create: (key, value) =>
				Effect.gen(function* () {
					const { path, tempPath } = yield* prepareWrite(key);
					yield* fs.writeFile(tempPath, value).pipe(Effect.orDie);
					// link(2) fails with EEXIST when the target exists, so the existence check and
					// the publication of the complete file are a single atomic step.
					const linked = yield* fs.link(tempPath, path).pipe(
						Effect.as(true),
						Effect.catch((error) =>
							error.reason._tag === "AlreadyExists"
								? Effect.succeed(false)
								: Effect.die(error),
						),
						Effect.ensuring(fs.remove(tempPath, { force: true }).pipe(Effect.orDie)),
					);

					if (!linked) {
						yield* Effect.logDebug(`fileKV.create(${key}) => ConflictError`);
						return yield* new ConflictError({ key });
					}

					return { etag: yield* getEtag(value) };
				}),
			delete: (key) =>
				fs.remove(`${rootDirectory}/${key}`, { force: true }).pipe(
					Effect.orDie,
					Effect.tap(() => Effect.logDebug(`fileKV.delete(${key})`)),
				),
			list: (prefix) =>
				Effect.promise(async () => {
					const entries = await readdir(rootDirectory, {
						recursive: true,
						withFileTypes: true,
					});
					const objects: ListedObject[] = [];
					for (const entry of entries) {
						if (!entry.isFile()) {
							continue;
						}
						const path = join(entry.parentPath, entry.name);
						const key = relative(rootDirectory, path);
						if (
							key.startsWith(prefix) &&
							!key.endsWith(".tmp") &&
							!key.endsWith(".lock")
						) {
							objects.push({ key, lastModified: (await stat(path)).mtimeMs });
						}
					}
					return objects;
				}),
			readStream: (key) =>
				Effect.gen(function* () {
					const path = `${rootDirectory}/${key}`;
					if (!(yield* fs.exists(path).pipe(Effect.orDie))) {
						return Option.none();
					}

					return Option.some(fs.stream(path).pipe(Stream.orDie));
				}),
			writeStream: (key, content) =>
				Effect.gen(function* () {
					const { path, tempPath } = yield* prepareWrite(key);
					yield* Stream.run(content, fs.sink(tempPath)).pipe(Effect.orDie);
					yield* fs.rename(tempPath, path).pipe(Effect.orDie);
					const value = yield* fs.readFile(path).pipe(Effect.orDie);
					const etag = yield* getEtag(value);
					yield* Effect.logDebug(`fileKV.writeStream(${key}) => ${etag}`);
					return { etag };
				}),
			clone: (from, to) =>
				Effect.gen(function* () {
					// Copy into a temporary file first, so a crash never leaves a truncated `to`.
					const { path: toPath, tempPath } = yield* prepareWrite(to);
					const fromPath = `${rootDirectory}/${from}`;
					// COPYFILE_FICLONE falls back to a regular copy when reflinks are unsupported.
					yield* Effect.tryPromise({
						try: () => copyFilePromise(fromPath, tempPath, constants.COPYFILE_FICLONE),
						catch: (error) =>
							new Error(`Failed to clone ${from} to ${to}: ${String(error)}`),
					}).pipe(Effect.orDie);
					yield* fs.rename(tempPath, toPath).pipe(Effect.orDie);
					yield* Effect.logDebug(`fileKV.clone(${from}, ${to})`);
				}),
			move: (from, to) =>
				Effect.gen(function* () {
					const { path: toPath } = yield* prepareWrite(to);
					yield* fs.rename(`${rootDirectory}/${from}`, toPath).pipe(Effect.orDie);
					yield* Effect.logDebug(`fileKV.move(${from}, ${to})`);
				}),
			open: openDatabase,
			connect: (key) =>
				Effect.acquireRelease(openDatabase(key), (db) => Effect.promise(() => db.close())),
		};
	});