import { Context, Effect, Option } from "effect";

import type { CloneTrait, ConnectDatabaseTrait, KV } from "./kv/kv";
import type { Head } from "./types";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export class LocalKV extends Context.Service<LocalKV, KV & CloneTrait & ConnectDatabaseTrait>()(
	"s3qlite/LocalKV",
) {}

export class RemoteKV extends Context.Service<RemoteKV, KV>()("s3qlite/RemoteKV") {}

export const headKey = (dbName: string): string => `${dbName}.json`;

export const batchKey = (batchId: string): string => `${batchId}.batch`;

export const snapshotKey = (snapshotId: string): string => `${snapshotId}.snapshot`;

export const dbKey = (dbName: string): string => `${dbName}.db`;

export const walKey = (dbName: string): string => `${dbName}.db-wal`;

export const shmKey = (dbName: string): string => `${dbName}.db-shm`;

export const baseKey = (snapshotId: string, batchId: string): string =>
	`${snapshotId}-${batchId}.base`;

/**
 * Stored head written during a pull before the live database is switched. Its presence marks the
 * pull as committed: recovery rolls the publication forward.
 */
export const pendingHeadKey = (dbName: string): string => `${dbName}.json.pending`;

/**
 * Fully built and checkpointed replacement for the live database, waiting to be renamed over it.
 */
export const stagedDbKey = (dbName: string): string => `${dbName}.db.staged`;

/**
 * Local file holding the database state at the end of `head` without local changes. A head with
 * no batches after its latest snapshot uses the snapshot itself.
 */
export const rollbackBaseKey = (head: Head): Option.Option<string> =>
	Option.fromNullishOr(head.snapshots.at(-1)).pipe(
		Option.map((snapshot) => {
			const lastBatchId = head.batches.at(-1)?.id;
			return !lastBatchId || lastBatchId === snapshot.batchIdsApplied.at(-1)
				? snapshotKey(snapshot.id)
				: baseKey(snapshot.id, lastBatchId);
		}),
	);

export const encodeJson = <A>(value: A): Uint8Array => textEncoder.encode(JSON.stringify(value));

export const decodeJson = <A>(value: Uint8Array): Effect.Effect<A, SyntaxError> =>
	Effect.try({
		try: () => JSON.parse(textDecoder.decode(value)) as A,
		catch: (error) =>
			error instanceof SyntaxError
				? error
				: new SyntaxError(`Failed to parse "${value}": ${String(error)}`),
	});

export const getJson = <A>(
	kv: KV,
	key: string,
): Effect.Effect<Option.Option<{ value: A; etag: string }>, SyntaxError> =>
	kv.get(key).pipe(
		Effect.flatMap(
			Option.match({
				onNone: () => Effect.succeed(Option.none()),
				onSome: ({ etag, value }) =>
					decodeJson<A>(value).pipe(
						Effect.map((decoded) => Option.some({ etag, value: decoded })),
					),
			}),
		),
	);