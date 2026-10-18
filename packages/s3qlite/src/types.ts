import type { Database } from "@tursodatabase/database";
import { Data } from "effect";
import type { Effect } from "effect";

import type { ConflictError } from "./kv/kv";

export class SameNameError extends Data.TaggedError("SameNameError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

export class AlreadyExistsError extends Data.TaggedError("AlreadyExistsError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

export class SourceDoesNotExistError extends Data.TaggedError("SourceDoesNotExistError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

export type ForkError = SameNameError | AlreadyExistsError | SourceDoesNotExistError;

export type Snapshot = {
	id: string;

	// thanks to this we can do full history
	baseSnapshotId?: string;
	batchIdsApplied: string[];
};

export type Batch = {
	id: string;
};

export type Head = {
	snapshots: Snapshot[];
	batches: Batch[];
	/**
	 * Owner mode: incremented by every ownership claim. A writer that still holds an older version
	 * of the head can no longer win the head compare-and-set.
	 */
	generation?: number;
	/** Owner mode: id of the instance that claimed the head last (diagnostics only). */
	owner?: string;
};

export type PendingBatch = {
	id: string;
	lastChangeId: number;
};

export type StoredHead = {
	head: Head;

	/**
	 * The latest etag of the remote head this instance has been synced to.
	 */
	remoteEtag: string;

	lastSyncedLocalChangeId: number;

	pendingBatches?: PendingBatch[];
};

/**
 * When a write is acknowledged in owner mode.
 *
 * - `durable`: after its changes are pushed and the head compare-and-set succeeded.
 * - `fast`: at once; a background push ships the changes shortly after (see `pushDelayMs`).
 */
export type ReplyMode = "durable" | "fast";

export type OwnerSnapshotPolicy = {
	/** Take a snapshot when this many batches follow the latest snapshot. Default 64. */
	everyBatches?: number;
	/** Take a snapshot when the batches pushed since the latest snapshot reach this size. Default 64 MiB. */
	everyBytes?: number;
};

export type OwnerGcPolicy = {
	/**
	 * Only delete unreferenced objects older than this. It protects uploads of a newer owner that
	 * are not referenced by the head yet, and readers that restore an older head. Default 1 hour.
	 */
	minAgeMs?: number;
};

/**
 * Single-writer mode. See `docs/owner-mode.md`.
 */
export type OwnerSyncMode = {
	type: "owner";
	/** Default reply mode of statements; override per call with `ReplyMode.durable/fast`. Default `durable`. */
	reply?: ReplyMode;
	/** Fast mode coalescing window: wait this long after a write before pushing. Default 100 ms. */
	pushDelayMs?: number;
	/** Periodic snapshot and head compaction. `false` disables it. */
	snapshot?: false | OwnerSnapshotPolicy;
	/** Garbage collection of unreferenced objects after a snapshot. Disabled by default. */
	gc?: false | OwnerGcPolicy;
	/** Recorded in the head on claim. Default: a random id. */
	ownerId?: string;
};

export type SyncMode =
	| "manual"
	| { type: "interval"; everyMs: number }
	| { type: "transaction" }
	| { type: "push-on-change" }
	| OwnerSyncMode;

/**
 * Owner mode: another instance claimed the database (or the head changed underneath). The instance
 * stops accepting statements; its unpushed changes are not published.
 */
export class OwnershipLostError extends Data.TaggedError("OwnershipLostError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

/**
 * Owner mode: a push failed for another reason (for example the object store is down). The outcome
 * of the writes it covered is unknown: they are committed locally and a later push may still
 * publish them. Retry with the same write id (see `Idempotency`).
 */
export class PushFailedError extends Data.TaggedError("PushFailedError")<{
	readonly dbName: string;
	readonly message: string;
	readonly cause: unknown;
}> {}

/** The operation is not available in the database's sync mode (for example `pull` in owner mode). */
export class SyncModeError extends Data.TaggedError("SyncModeError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

export type OwnerError = OwnershipLostError | PushFailedError;

// export type ConflictResolutionMode = "fail" | "preferRemote" | "preferLocal";
//
// export type ConflictOperation = "pull" | "push" | "sync";
//
// export type ConflictResolutionContext = {
// 	readonly cause: Error;
// 	readonly localHead: StoredHead;
// 	readonly operation: ConflictOperation;
// 	readonly remoteHead: Head;
// };
//
// export type ConflictResolver = (
// 	context: ConflictResolutionContext,
// ) => Effect.Effect<ConflictResolutionMode, Error>;
//
// type StrongConsistencyOptions = {
// 	mode: "strong";
// };
//
// type EventualConsistencyOptions = {
// 	mode: "eventual";
// 	sync?: "manual" | { everyMs: number };
// };

export type ConnectionOptions = {
	bucket: string;
	// conflictResolution?: ConflictResolutionMode | ConflictResolver;
	localDirectory?: string;
	syncMode?: SyncMode;
	/**
	 * Push unpushed local changes (pull first if needed) when the connection closes.
	 * Defaults to `true` for every sync mode except `"manual"`.
	 */
	pushOnClose?: boolean;
};
// } & (StrongConsistencyOptions | EventualConsistencyOptions);

export type S3qliteDatabase = Database & {
	pull: () => Effect.Effect<void, Error>;
	push: () => Effect.Effect<void, ConflictError | OwnerError>;
	sync: () => Effect.Effect<void, Error | ConflictError>;
	fork: (
		dbName: string,
	) => Effect.Effect<void, SameNameError | AlreadyExistsError | SourceDoesNotExistError>;
	checkpoint: () => Effect.Effect<void, Error>;
};