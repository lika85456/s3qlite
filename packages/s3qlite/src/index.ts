import { Effect } from "effect";

import { connect as connectInternal, initializeContexts } from "./connection";
import type { ConnectionOptions } from "./types";

export * from "./cdc/apply";
export * from "./cdc/extract";
export * from "./cdc/types";
export * from "./kv/fileKV";
export * from "./kv/kv";
export * from "./kv/memoryKV";
export * from "./kv/s3KV";

export { initializeContexts as initContexts };

export const connect = (dbName: string, connectionOptions: ConnectionOptions) =>
	initializeContexts(connectionOptions).pipe(
		Effect.flatMap((ctx) =>
			connectInternal(dbName, connectionOptions).pipe(Effect.provide(ctx)),
		),
	);