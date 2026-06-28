import type { Database } from "@tursodatabase/database";
import { Data } from "effect";
import type { Effect, Option, Scope, Stream } from "effect";

export class ConflictError extends Data.TaggedError("ConflictError")<{
	readonly key: string;
}> {}

export type KV = {
	get(key: string): Effect.Effect<Option.Option<{ value: Uint8Array; etag: string }>>;
	/** Optimization for S3 calls, so there isn't head+get, but only one request*/
	getIfChanged(
		key: string,
		etag: string,
	): Effect.Effect<Option.Option<{ value: Uint8Array; etag: string }>>;
	exists(key: string): Effect.Effect<boolean>;
	set(key: string, value: Uint8Array): Effect.Effect<{ etag: string }>;
	cas(
		key: string,
		value: Uint8Array,
		etag: string,
	): Effect.Effect<{ etag: string }, ConflictError>;
	delete(key: string): Effect.Effect<void>;

	readStream(key: string): Effect.Effect<Option.Option<Stream.Stream<Uint8Array>>>;
	writeStream(key: string, content: Stream.Stream<Uint8Array>): Effect.Effect<{ etag: string }>;
};

export type CloneTrait = {
	clone(from: string, to: string): Effect.Effect<void>;
};

export type ConnectDatabaseTrait = {
	/**
	 * Creates connection with CDC full enabled
	 */
	connect(key: string): Effect.Effect<Database, Error, Scope.Scope>;
};
