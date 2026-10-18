import type { S3Service } from "@effect-aws/client-s3";
import { Effect } from "effect";
import type { Scope } from "effect";
import type { FileSystem } from "effect/FileSystem";

import { connect as connectInternal, initializeContexts } from "./connection";
import { fromInternal } from "./from";
import type { FileNotFoundError } from "./from";
import type { AlreadyExistsError, ConnectionOptions, S3qliteDatabase } from "./types";

// Public API only. CDC, KV and head/batch internals are not exported.
export { BucketDoesNotExistError } from "./kv/s3KV";
export { ConflictError } from "./kv/kv";
export { ReplayError } from "./cdc/apply";
export { FileNotFoundError } from "./from";
export {
	AlreadyExistsError,
	OwnershipLostError,
	PushFailedError,
	SameNameError,
	SourceDoesNotExistError,
	SyncModeError,
} from "./types";
export type {
	ConnectionOptions,
	ForkError,
	OwnerError,
	OwnerGcPolicy,
	OwnerSnapshotPolicy,
	OwnerSyncMode,
	ReplyMode,
	S3qliteDatabase,
	SyncMode,
} from "./types";
export * as S3qliteClient from "./sql/public";
export { S3qlite, ownerErrorOf } from "./sql/client";
export type { S3qliteClientConfig, S3qliteService } from "./sql/client";
export * as Reply from "./replyMode";
export { HeadStore } from "./owner/headStore";
export type { HeadStoreShape, VersionedHead } from "./owner/headStore";
export type { OwnerOpenStats, OwnerStatus } from "./owner/owner";

export const connect = (
	dbName: string,
	connectionOptions: ConnectionOptions,
): Effect.Effect<S3qliteDatabase, Error, Scope.Scope | FileSystem | S3Service> =>
	initializeContexts(connectionOptions).pipe(
		Effect.flatMap((ctx) =>
			connectInternal(dbName, connectionOptions).pipe(Effect.provideContext(ctx)),
		),
	);

export const from = (
	pathToFile: string,
	dbName: string,
	connectionOptions: ConnectionOptions,
): Effect.Effect<
	void,
	AlreadyExistsError | FileNotFoundError | Error,
	Scope.Scope | FileSystem | S3Service
> =>
	initializeContexts(connectionOptions).pipe(
		Effect.flatMap((ctx) => fromInternal(pathToFile, dbName).pipe(Effect.provideContext(ctx))),
	);