import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { layer as pathLayer } from "@effect/platform-node/NodePath";
import type { Database } from "@tursodatabase/database";
import { Effect } from "effect";

import { connect as connectEffect } from "./src/index";
import type { ConnectionOptions, S3qliteDatabase } from "./src/types";

type PromiseMethods = {
	pull: () => Promise<void>;
	push: () => Promise<void>;
	sync: () => Promise<void>;
	fork: (dbName: string) => Promise<void>;
	checkpoint: () => Promise<void>;
};

export type PromiseS3qliteDatabase = Database & PromiseMethods;

export type PromiseConnectionOptions = ConnectionOptions & {
	s3?: Parameters<typeof S3.layer>[0];
};

const toPromiseDatabase = (database: S3qliteDatabase): PromiseS3qliteDatabase => {
	const pull = database.pull.bind(database);
	const push = database.push.bind(database);
	const sync = database.sync.bind(database);
	const fork = database.fork.bind(database);
	const checkpoint = database.checkpoint.bind(database);

	return Object.assign(database as Database, {
		pull: () => Effect.runPromise(pull()),
		push: () => Effect.runPromise(push()),
		sync: () => Effect.runPromise(sync()),
		fork: (dbName: string) => Effect.runPromise(fork(dbName)),
		checkpoint: () => Effect.runPromise(checkpoint()),
	}) as PromiseS3qliteDatabase;
};

export const connect = async (
	dbName: string,
	{ s3, ...connectionOptions }: PromiseConnectionOptions,
): Promise<PromiseS3qliteDatabase> =>
	Effect.runPromise(
		connectEffect(dbName, connectionOptions).pipe(
			Effect.provide(fileSystemLayer),
			Effect.provide(pathLayer),
			Effect.provide(S3.layer(s3 ?? {})),
			Effect.map(toPromiseDatabase),
		),
	);

export * from "./src/index.ts";