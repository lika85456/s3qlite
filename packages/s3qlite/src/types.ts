import type { Database } from "@tursodatabase/database";
import type { Effect } from "effect";

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
};
// } & (StrongConsistencyOptions | EventualConsistencyOptions);

export type S3qliteDatabase = Database & {
	pull: () => Effect.Effect<void, Error>;
	push: () => Effect.Effect<void, Error>;
	sync: () => Effect.Effect<void, Error>;
	fork: (dbName: string) => Effect.Effect<void, Error>;
	checkpoint: () => Effect.Effect<void, Error>;
};