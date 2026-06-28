import { FileSystem } from "@effect/platform/FileSystem";
import { connect as connectTurso } from "@tursodatabase/database";
import type { Scope } from "effect";
import { Effect, Option, Stream } from "effect";
import { constants } from "node:fs";
import { copyFile as copyFilePromise } from "node:fs/promises";

import { ConflictError } from "./kv";
import type { CloneTrait, ConnectDatabaseTrait, KV } from "./kv";

// TODO: in the future the etag should be probably cached
const getEtag = (value: Uint8Array): Effect.Effect<string> =>
	Effect.promise(async () => {
		const digest = await crypto.subtle.digest("SHA-256", Uint8Array.from(value).buffer);
		return Array.from(new Uint8Array(digest), (byte) =>
			byte.toString(16).padStart(2, "0"),
		).join("");
	}).pipe(Effect.orDie);

export const makeFileKV = (
	rootDirectory: string,
): Effect.Effect<KV & CloneTrait & ConnectDatabaseTrait, never, FileSystem | Scope.Scope> =>
	Effect.gen(function* () {
		const fs = yield* FileSystem;

		const writeBytes = (key: string, value: Uint8Array): Effect.Effect<{ etag: string }> =>
			Effect.gen(function* () {
				const path = `${rootDirectory}/${key}`;
				const directory = path.split("/").slice(0, -1).join("/");
				yield* fs.makeDirectory(directory, { recursive: true }).pipe(Effect.orDie);
				const tempPath = `${path}.${crypto.randomUUID()}.tmp`;
				yield* fs.writeFile(tempPath, value).pipe(Effect.orDie);
				yield* fs.rename(tempPath, path).pipe(Effect.orDie);
				return { etag: yield* getEtag(value) };
			});

		return {
			get: (key) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`fileKV.get(${key})`);

					const path = `${rootDirectory}/${key}`;
					if (!(yield* fs.exists(path).pipe(Effect.orDie))) {
						yield* Effect.logDebug(`fileKV.get(${key}) => None`);
						return Option.none();
					}

					const value = yield* fs.readFile(path).pipe(Effect.orDie);
					const etag = yield* getEtag(value);
					yield* Effect.logDebug(`fileKV.get(${key}) => Some(${etag})`);
					return Option.some({ etag, value });
				}),
			getIfChanged: (key, etag) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`fileKV.getIfChanged(${key}, ${etag})`);

					const path = `${rootDirectory}/${key}`;
					if (!(yield* fs.exists(path).pipe(Effect.orDie))) {
						yield* Effect.logDebug(`fileKV.getIfChanged(${key}) => None`);
						return Option.none();
					}

					const value = yield* fs.readFile(path).pipe(Effect.orDie);
					const currentEtag = yield* getEtag(value);
					if (currentEtag === etag) {
						yield* Effect.logDebug(`fileKV.getIfChanged(${key}) => None (not changed)`);
						return Option.none();
					}

					yield* Effect.logDebug(`fileKV.getIfChanged(${key}) => Some(${currentEtag})`);
					return Option.some({ etag: currentEtag, value });
				}),
			exists: (key) =>
				Effect.logDebug(`fileKV.exists(${key})`).pipe(
					Effect.flatMap(() => fs.exists(`${rootDirectory}/${key}`).pipe(Effect.orDie)),
					Effect.tap((result) => Effect.logDebug(`fileKV.exists(${key}) => ${result}`)),
				),
			set: (key, value) =>
				Effect.logDebug(`fileKV.set(${key}, ${value.length} bytes)`).pipe(
					Effect.flatMap(() => writeBytes(key, value)),
					Effect.tap((result) => Effect.logDebug(`fileKV.set(${key}) => ${result.etag}`)),
				),
			cas: (key, value, etag) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`fileKV.cas(${key}, ${value.length} bytes, ${etag})`);

					const path = `${rootDirectory}/${key}`;
					if (!(yield* fs.exists(path).pipe(Effect.orDie))) {
						yield* Effect.logDebug(`fileKV.cas(${key}) => ConflictError (missing)`);
						return yield* Effect.fail(new ConflictError({ key }));
					}

					const existingValue = yield* fs.readFile(path).pipe(Effect.orDie);
					if ((yield* getEtag(existingValue)) !== etag) {
						yield* Effect.logDebug(
							`fileKV.cas(${key}) => ConflictError (etag mismatch)`,
						);
						return yield* Effect.fail(new ConflictError({ key }));
					}

					const result = yield* writeBytes(key, value);
					yield* Effect.logDebug(`fileKV.cas(${key}) => ${result.etag}`);
					return result;
				}),
			delete: (key) =>
				Effect.logDebug(`fileKV.delete(${key})`).pipe(
					Effect.flatMap(() =>
						fs.remove(`${rootDirectory}/${key}`, { force: true }).pipe(Effect.orDie),
					),
					Effect.tap(() => Effect.logDebug(`fileKV.delete(${key}) => void`)),
				),
			readStream: (key) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`fileKV.readStream(${key})`);

					const path = `${rootDirectory}/${key}`;
					if (!(yield* fs.exists(path).pipe(Effect.orDie))) {
						yield* Effect.logDebug(`fileKV.readStream(${key}) => None`);
						return Option.none();
					}

					yield* Effect.logDebug(`fileKV.readStream(${key}) => Some(stream)`);
					return Option.some(fs.stream(path).pipe(Stream.orDie));
				}),
			writeStream: (key, content) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`fileKV.writeStream(${key}, stream)`);

					const path = `${rootDirectory}/${key}`;
					const directory = path.split("/").slice(0, -1).join("/");
					yield* fs.makeDirectory(directory, { recursive: true }).pipe(Effect.orDie);
					const tempPath = `${path}.${crypto.randomUUID()}.tmp`;
					yield* Stream.run(content, fs.sink(tempPath)).pipe(Effect.orDie);
					yield* fs.rename(tempPath, path).pipe(Effect.orDie);
					const value = yield* fs.readFile(path).pipe(Effect.orDie);
					const etag = yield* getEtag(value);
					yield* Effect.logDebug(`fileKV.writeStream(${key}) => ${etag}`);
					return { etag };
				}),
			clone: (from, to) =>
				Effect.gen(function* () {
					yield* Effect.logDebug(`fileKV.clone(${from}, ${to})`);

					const fromPath = `${rootDirectory}/${from}`;
					const toPath = `${rootDirectory}/${to}`;
					const directory = toPath.split("/").slice(0, -1).join("/");
					yield* fs.makeDirectory(directory, { recursive: true }).pipe(Effect.orDie);
					const cloneAttempt = yield* Effect.either(
						Effect.promise(() =>
							copyFilePromise(fromPath, toPath, constants.COPYFILE_FICLONE),
						),
					);

					if (cloneAttempt._tag === "Right") {
						yield* Effect.logDebug(`fileKV.clone(${from}, ${to}) => void (copied)`);
						return;
					}

					const errorOption =
						typeof cloneAttempt.left === "object"
							? Option.fromNullable(cloneAttempt.left)
							: Option.none();
					const currentError = Option.map(
						errorOption,
						(error) => error as { code?: unknown },
					);

					if (
						Option.isNone(currentError) ||
						!("code" in currentError.value) ||
						!["ENOTSUP", "EINVAL", "ENOSYS", "EXDEV", "EOPNOTSUPP"].includes(
							String(currentError.value.code),
						)
					) {
						return yield* Effect.die(cloneAttempt.left);
					}

					yield* fs.copyFile(fromPath, toPath).pipe(Effect.orDie);
					yield* Effect.logDebug(`fileKV.clone(${from}, ${to}) => void (copied)`);
				}),
			connect: (key) =>
				Effect.logDebug(`fileKV.connect(${key})`).pipe(
					Effect.flatMap(() =>
						Effect.acquireRelease(
							Effect.tryPromise({
								try: async () => {
									const connection = await connectTurso(
										`${rootDirectory}/${key}`,
									);
									await connection.exec(
										"PRAGMA capture_data_changes_conn('full')",
									);
									return connection;
								},
								catch: (e) =>
									new Error(
										`Failed to connect to database at key ${key}: ${String(e)}`,
									),
							}),
							(db) => Effect.promise(() => db.close()),
						),
					),
					Effect.tap((result) => Effect.logDebug(`fileKV.connect(${key}) => database`)),
				),
		};
	});
