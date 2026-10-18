import type { Database } from "@tursodatabase/database";
import { Data } from "effect";
import type { Effect, Option, Scope, Stream } from "effect";

export class ConflictError extends Data.TaggedError("ConflictError")<{
	readonly key: string;
}> {}

export type ListedObject = {
	readonly key: string;
	readonly lastModified: number;
};

export type KV = {
	get(key: string): Effect.Effect<Option.Option<{ value: Uint8Array; etag: string }>>;
	/** Optimization for S3 calls, so there isn't head+get, but only one request*/
	getIfChanged(
		key: string,
		etag: string,
	): Effect.Effect<Option.Option<{ value: Uint8Array; etag: string }>>;
	exists(key: string): Effect.Effect<boolean>;
	set(key: string, value: Uint8Array): Effect.Effect<{ etag: string }>;
	/**
	 * Replaces `key` only when its current etag equals `etag` (S3 `If-Match`).
	 * Fails with `ConflictError` when the key is missing or the etag differs.
	 */
	cas(
		key: string,
		value: Uint8Array,
		etag: string,
	): Effect.Effect<{ etag: string }, ConflictError>;
	/**
	 * Writes `key` only when it does not exist yet (S3 `If-None-Match: *`).
	 * Fails with `ConflictError` when the key already exists.
	 */
	create(key: string, value: Uint8Array): Effect.Effect<{ etag: string }, ConflictError>;
	delete(key: string): Effect.Effect<void>;

	/**
	 * Lists the keys that start with `prefix`, with their last modification time in epoch
	 * milliseconds. Optional; owner-mode garbage collection needs it.
	 */
	list?(prefix: string): Effect.Effect<readonly ListedObject[]>;

	readStream(key: string): Effect.Effect<Option.Option<Stream.Stream<Uint8Array>>>;
	writeStream(key: string, content: Stream.Stream<Uint8Array>): Effect.Effect<{ etag: string }>;
};

export type CloneTrait = {
	/** Copies `from` to `to`. Readers never observe a partially written `to`. */
	clone(from: string, to: string): Effect.Effect<void>;
	/** Atomically renames `from` to `to`, replacing `to` if it exists. */
	move(from: string, to: string): Effect.Effect<void>;
};

export type ConnectDatabaseTrait = {
	/**
	 * Opens a connection with CDC full enabled. The caller owns the connection and must close it.
	 */
	open(key: string): Effect.Effect<Database, Error>;
	/**
	 * Creates connection with CDC full enabled, closed when the scope closes.
	 */
	connect(key: string): Effect.Effect<Database, Error, Scope.Scope>;
};