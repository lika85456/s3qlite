import { FileSystem } from "@effect/platform/FileSystem";
import { Data, Effect, Option, Stream } from "effect";
import type { Scope } from "effect";

import { truncate } from "./cdc/truncate";
import { AlreadyExistsError } from "./fork";
import { LocalKV, RemoteKV, dbKey, encodeJson, headKey, snapshotKey } from "./storage";
import type { Head, StoredHead } from "./types";

export class FileNotFoundError extends Data.TaggedError("FileNotFoundError")<{
	readonly path: string;
	readonly message: string;
}> {}

export const fromInternal = (
	pathToFile: string,
	dbName: string,
): Effect.Effect<
	void,
	AlreadyExistsError | FileNotFoundError | Error,
	RemoteKV | LocalKV | FileSystem | Scope.Scope
> =>
	Effect.gen(function* () {
		const remote = yield* RemoteKV;
		const local = yield* LocalKV;
		const fs = yield* FileSystem;

		const existingHead = yield* remote.get(headKey(dbName));
		if (Option.isSome(existingHead)) {
			return yield* Effect.fail(
				new AlreadyExistsError({
					dbName,
					message: `Database "${dbName}" already exists`,
				}),
			);
		}

		const sourceExists = yield* fs.exists(pathToFile).pipe(Effect.orDie);
		if (!sourceExists) {
			return yield* Effect.fail(
				new FileNotFoundError({
					path: pathToFile,
					message: `File "${pathToFile}" does not exist`,
				}),
			);
		}

		const stream = fs.stream(pathToFile).pipe(Stream.orDie);
		yield* local.writeStream(dbKey(dbName), stream);

		const connection = yield* local.connect(dbKey(dbName));
		yield* truncate(connection);
		yield* Effect.tryPromise(() => connection.close());

		const snapshotId = yield* Effect.sync(() => crypto.randomUUID());
		const head: Head = {
			snapshots: [{ id: snapshotId, batchIdsApplied: [] }],
			batches: [],
		};

		const snapshotStream = yield* local.clone(dbKey(dbName), snapshotKey(snapshotId)).pipe(
			Effect.flatMap(() => local.readStream(snapshotKey(snapshotId))),
			Effect.flatMap(
				Option.match({
					onNone: () => Effect.die(new Error("Snapshot not found after clone")),
					onSome: (stream) => Effect.succeed(stream),
				}),
			),
		);
		yield* remote.writeStream(snapshotKey(snapshotId), snapshotStream);

		const remoteResult = yield* remote.set(headKey(dbName), encodeJson(head));

		yield* local.set(
			headKey(dbName),
			encodeJson<StoredHead>({
				head,
				remoteEtag: remoteResult.etag,
				lastSyncedLocalChangeId: 0,
			}),
		);
	});