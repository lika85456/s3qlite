import type { S3Service } from "@effect-aws/client-s3";
import type { FileSystem } from "@effect/platform/FileSystem";
import { Effect } from "effect";
import type { Scope } from "effect";

import { connect as connectInternal, initializeContexts } from "./connection";
import type { ConnectionOptions, S3qliteDatabase } from "./types";

export * from "./cdc/apply";
export * from "./cdc/extract";
export * from "./cdc/types";
export * from "./fork";
export * from "./kv/fileKV";
export * from "./kv/kv";
export * from "./kv/memoryKV";
export * from "./kv/s3KV";
export * from "./types";

export { initializeContexts as initContexts };

export const connect = (
	dbName: string,
	connectionOptions: ConnectionOptions,
): Effect.Effect<S3qliteDatabase, Error, Scope.Scope | FileSystem | S3Service> =>
	initializeContexts(connectionOptions).pipe(
		Effect.flatMap((ctx) =>
			connectInternal(dbName, connectionOptions).pipe(Effect.provide(ctx)),
		),
	);
