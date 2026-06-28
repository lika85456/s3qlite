import { Context, Effect, Option } from "effect";

import type { CloneTrait, ConnectDatabaseTrait, KV } from "./kv/kv";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export class LocalKV extends Context.Tag("s3qlite/LocalKV")<
	LocalKV,
	KV & CloneTrait & ConnectDatabaseTrait
>() {}

export class RemoteKV extends Context.Tag("s3qlite/RemoteKV")<RemoteKV, KV>() {}

export const headKey = (dbName: string): string => `${dbName}.json`;

export const batchKey = (batchId: string): string => `${batchId}.batch`;

export const snapshotKey = (snapshotId: string): string => `${snapshotId}.snapshot`;

export const dbKey = (dbName: string): string => `${dbName}.db`;

export const walKey = (dbName: string): string => `${dbName}.db-wal`;

export const shmKey = (dbName: string): string => `${dbName}.db-shm`;

export const baseKey = (snapshotId: string, batchId: string): string =>
	`${snapshotId}-${batchId}.base`;

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