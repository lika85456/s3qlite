import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import type { Database } from "@tursodatabase/database";
import { Effect, Exit, Layer, Scope } from "effect";
import type { SqlClient } from "effect/sql/SqlClient";

import { connect as connectEffect, from as fromEffect } from "./src/index";
import * as S3qliteClient from "./src/sql/client";
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
): Promise<PromiseS3qliteDatabase> => {
	const scope = Scope.makeUnsafe();

	try {
		const database = await Effect.runPromise(
			connectEffect(dbName, connectionOptions).pipe(
				Effect.provide(fileSystemLayer),
				Effect.provide(S3.layer(s3 ?? {})),
				Scope.provide(scope),
			),
		);

		const wrapped = toPromiseDatabase(database);
		wrapped.close = () => Effect.runPromise(Scope.close(scope, Exit.void));
		return wrapped;
	} catch (error) {
		await Effect.runPromise(Scope.close(scope, Exit.void));
		throw error;
	}
};

export const from = async (
	pathToFile: string,
	dbName: string,
	{ s3, ...connectionOptions }: PromiseConnectionOptions,
): Promise<void> => {
	const scope = Scope.makeUnsafe();

	try {
		await Effect.runPromise(
			fromEffect(pathToFile, dbName, connectionOptions).pipe(
				Effect.provide(fileSystemLayer),
				Effect.provide(S3.layer(s3 ?? {})),
				Scope.provide(scope),
			),
		);
	} finally {
		await Effect.runPromise(Scope.close(scope, Exit.void));
	}
};

export type SqlClientLayerOptions = S3qliteClient.S3qliteClientConfig & {
	s3?: Parameters<typeof S3.layer>[0];
};

/**
 * `S3qliteClient.layer` with Node's file system and an `S3` client built from `s3` options.
 * Provides the standard Effect `SqlClient` plus the `S3qlite` sync-control service.
 */
export const sqlClientLayer = ({
	s3,
	...config
}: SqlClientLayerOptions): Layer.Layer<
	S3qliteClient.S3qliteClient | SqlClient | S3qliteClient.S3qlite,
	Error
> =>
	S3qliteClient.layer(config).pipe(
		Layer.provide(fileSystemLayer),
		Layer.provide(S3.layer(s3 ?? {})),
	);

export * from "./src/index.js";