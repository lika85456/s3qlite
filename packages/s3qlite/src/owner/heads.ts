/** Pure helpers of owner mode: object keys, head arithmetic and CDC pruning. */
import type { Database } from "@tursodatabase/database";
import { Effect, Option } from "effect";

import { batchKey, snapshotKey } from "../storage";
import type { Head } from "../types";

/** Prefix of every object an owner uploads; garbage collection only looks below it. */
export const objectPrefix = (dbName: string): string => `${dbName}.objects/`;

/** Marker written by `fork`; garbage collection keeps objects referenced by forks. */
export const forkMarkerPrefix = (dbName: string): string => `${dbName}.forks/`;

export const newObjectId = (dbName: string): string =>
	`${objectPrefix(dbName)}${crypto.randomUUID()}`;

export const sameHead = (left: Head, right: Head): boolean =>
	JSON.stringify(left) === JSON.stringify(right);

export const headReferences = (head: Head): readonly string[] => [
	...head.snapshots.map((snapshot) => snapshotKey(snapshot.id)),
	...head.batches.map((batch) => batchKey(batch.id)),
];

/** Batches that follow the latest snapshot; restore replays exactly these. */
export const batchesAfterSnapshot = (head: Head): number => {
	const lastApplied = head.snapshots.at(-1)?.batchIdsApplied.at(-1);
	return Option.match(Option.fromNullishOr(lastApplied), {
		onNone: () => head.batches.length,
		onSome: (id) =>
			head.batches.length - 1 - head.batches.findIndex((batch) => batch.id === id),
	});
};

/**
 * Head after a snapshot that includes every batch of `head`. Only the newest snapshot is kept, and
 * of the batches only the last one, which the snapshot lists as applied (restore uses it to find
 * where to continue). This keeps the head small and restore bounded.
 */
export const compactHead = (head: Head, snapshotId: string): Head => {
	const lastBatch = head.batches.at(-1);
	const previous = head.snapshots.at(-1);
	return {
		...head,
		snapshots: [
			{
				id: snapshotId,
				...(previous ? { baseSnapshotId: previous.id } : {}),
				batchIdsApplied: lastBatch ? [lastBatch.id] : [],
			},
		],
		batches: lastBatch ? [lastBatch] : [],
	};
};

export const closeQuietly = (db: Database): Effect.Effect<void> =>
	Effect.tryPromise(() => db.close()).pipe(Effect.ignore);

/**
 * Prunes pushed CDC rows. The newest pushed row stays: Turso reuses change ids once the table is
 * empty, and the extractor and `lastSyncedLocalChangeId` rely on ids that only grow.
 */
export const pruneCdc = (db: Database, lastPushedChangeId: number): Effect.Effect<void> =>
	Effect.tryPromise(() =>
		db.run("DELETE FROM turso_cdc WHERE change_id < ?", lastPushedChangeId),
	).pipe(Effect.asVoid, Effect.orDie);