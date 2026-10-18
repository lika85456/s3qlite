import type { S3Service } from "@effect-aws/client-s3";
import { Context, Effect } from "effect";
import type { Scope } from "effect";
import type { FileSystem } from "effect/FileSystem";

import { makeEngine } from "./engine";
import type { S3qliteEngine } from "./engine";
import { makeFileKV } from "./kv/fileKV";
import type { BucketDoesNotExistError } from "./kv/s3KV";
import { makeS3KV } from "./kv/s3KV";
import { LocalKV, RemoteKV } from "./storage";
import type { ConnectionOptions, S3qliteDatabase } from "./types";
import { wrapDatabase } from "./wrapDatabase";

export const initializeContexts = (
	connectionOptions: ConnectionOptions,
): Effect.Effect<
	Context.Context<LocalKV | RemoteKV>,
	BucketDoesNotExistError,
	S3Service | FileSystem | Scope.Scope
> =>
	Effect.gen(function* () {
		const s3kv = yield* makeS3KV(connectionOptions.bucket);
		const filekv = yield* makeFileKV(connectionOptions.localDirectory ?? "./.s3qlite/");
		return Context.make(RemoteKV, s3kv).pipe(Context.add(LocalKV, filekv));
	});

const runCall = (engine: S3qliteEngine, call: () => unknown): Promise<unknown> =>
	Effect.runPromise(
		engine.semaphore
			.withPermit(
				engine.around(
					Effect.promise(() => Promise.resolve().then(call)),
					(error) => error,
				),
			)
			// Owner mode with durable replies: resolve only after the changes are pushed.
			.pipe(Effect.tap(() => engine.replyGate)),
	);

/**
 * Exposes the engine as a `@tursodatabase/database` `Database`. Every async method is queued
 * behind the engine semaphore and wrapped with the sync mode.
 */
export const wrapEngine = (engine: S3qliteEngine): S3qliteDatabase => {
	const wrapper = wrapDatabase(engine.currentConnection, (call, method) => {
		if (method === "close") {
			return Effect.runPromise(engine.close) as never;
		}

		if (method !== "iterate") {
			return runCall(engine, call) as never;
		}

		if (engine.syncMode.type !== "transaction") {
			return call();
		}

		// Iteration is lazy, so only the pull is queued; rows stream outside the semaphore.
		return (async function* () {
			await Effect.runPromise(engine.pull);
			for await (const row of call() as AsyncIterable<unknown>) {
				yield row;
			}
		})() as never;
	}) as unknown as S3qliteDatabase;

	wrapper.pull = () => engine.pull;
	wrapper.push = () => engine.push;
	wrapper.sync = () => engine.sync;
	wrapper.fork = engine.fork;
	wrapper.checkpoint = () => Effect.die(new Error("Not implemented"));

	return wrapper;
};

export const connect = (
	dbName: string,
	options: ConnectionOptions,
): Effect.Effect<S3qliteDatabase, Error, LocalKV | RemoteKV | Scope.Scope> =>
	makeEngine(dbName, options).pipe(Effect.map(wrapEngine));