import { Effect, Option } from "effect";

import { applyBatch } from "./batches";
import { getLatestChangeId } from "./cdc/extract";
import { truncate } from "./cdc/truncate";
import { pullFiles } from "./kv/syncFiles";
import { recoverPublication } from "./publication";
import {
	LocalKV,
	RemoteKV,
	baseKey,
	batchKey,
	dbKey,
	encodeJson,
	getJson,
	headKey,
	shmKey,
	snapshotKey,
	walKey,
} from "./storage";
import type { Head, StoredHead } from "./types";

/**
 * Replaces the local database with the remote head: latest snapshot plus the batches after it.
 */
export const joinRemote = (
	dbName: string,
	remoteHead: { readonly value: Head; readonly etag: string },
): Effect.Effect<void, Error, LocalKV | RemoteKV> =>
	Effect.gen(function* () {
		const local = yield* LocalKV;
		const head = remoteHead.value;
		const latestSnapshot = head.snapshots.at(-1);

		if (!latestSnapshot) {
			return yield* Effect.die(new Error("Remote head has no snapshots"));
		}

		const lastAppliedBatchId = latestSnapshot.batchIdsApplied.at(-1);
		const batchStartIndex = lastAppliedBatchId
			? head.batches.findIndex((b) => b.id === lastAppliedBatchId) + 1
			: 0;
		const batchesToDownload = head.batches.slice(batchStartIndex);

		yield* pullFiles([
			snapshotKey(latestSnapshot.id),
			...batchesToDownload.map((batch) => batchKey(batch.id)),
		]);

		yield* local.delete(walKey(dbName));
		yield* local.delete(shmKey(dbName));
		yield* local.clone(snapshotKey(latestSnapshot.id), dbKey(dbName));

		const lastSyncedLocalChangeId = yield* Effect.acquireUseRelease(
			local.open(dbKey(dbName)),
			(db) =>
				Effect.gen(function* () {
					for (const batch of batchesToDownload) {
						yield* applyBatch(db, batch);
					}
					const latestChangeId = yield* getLatestChangeId(db);
					yield* truncate(db);
					return latestChangeId;
				}),
			(db) => Effect.tryPromise(() => db.close()).pipe(Effect.ignore),
		);

		const lastBatchId = batchesToDownload.at(-1)?.id;
		if (lastBatchId) {
			yield* local.clone(dbKey(dbName), baseKey(latestSnapshot.id, lastBatchId));
		}

		yield* local.set(
			headKey(dbName),
			encodeJson<StoredHead>({
				head,
				remoteEtag: remoteHead.etag,
				lastSyncedLocalChangeId,
			}),
		);
	});

/**
 * Creates the database remotely. The snapshot is uploaded before the head, and the head is created
 * with `If-None-Match: *`, so a published head never points to a missing snapshot and two creators
 * cannot overwrite each other. Returns `false` when another creator won the race.
 */
const createRemote = (dbName: string): Effect.Effect<boolean, Error, LocalKV | RemoteKV> =>
	Effect.gen(function* () {
		const local = yield* LocalKV;
		const remote = yield* RemoteKV;
		const snapshotId = crypto.randomUUID();

		yield* Effect.acquireUseRelease(local.open(dbKey(dbName)), truncate, (db) =>
			Effect.tryPromise(() => db.close()).pipe(Effect.ignore),
		);
		yield* local.clone(dbKey(dbName), snapshotKey(snapshotId));

		const snapshotStream = yield* local.readStream(snapshotKey(snapshotId));
		if (Option.isNone(snapshotStream)) {
			return yield* Effect.die(new Error("Snapshot not found after clone"));
		}
		yield* remote.writeStream(snapshotKey(snapshotId), snapshotStream.value);

		const head: Head = {
			snapshots: [{ id: snapshotId, batchIdsApplied: [] }],
			batches: [],
		};

		const created = yield* remote.create(headKey(dbName), encodeJson(head)).pipe(
			Effect.map(Option.some),
			Effect.catchTag("ConflictError", () => Effect.succeed(Option.none())),
		);

		if (Option.isNone(created)) {
			yield* Effect.logInfo("s3qlite.create.lost-race").pipe(Effect.annotateLogs({ dbName }));
			yield* Effect.all([
				remote.delete(snapshotKey(snapshotId)),
				local.delete(snapshotKey(snapshotId)),
				local.delete(dbKey(dbName)),
				local.delete(walKey(dbName)),
				local.delete(shmKey(dbName)),
			]);
			return false;
		}

		yield* local.set(
			headKey(dbName),
			encodeJson<StoredHead>({
				head,
				remoteEtag: created.value.etag,
				lastSyncedLocalChangeId: 0,
			}),
		);
		return true;
	});

/**
 * Brings the local directory into a state where `{dbName}.db` matches `{dbName}.json`:
 * finishes interrupted pulls, creates the database when nobody has, or joins the remote head.
 */
export const bootstrap = (dbName: string): Effect.Effect<void, Error, LocalKV | RemoteKV> =>
	Effect.gen(function* () {
		const local = yield* LocalKV;
		const remote = yield* RemoteKV;

		yield* recoverPublication(dbName);

		const readRemoteHead = getJson<Head>(remote, headKey(dbName)).pipe(Effect.orDie);
		const localHead = yield* getJson<StoredHead>(local, headKey(dbName)).pipe(Effect.orDie);
		const remoteHead = yield* readRemoteHead;

		if (Option.isSome(localHead)) {
			if (Option.isNone(remoteHead)) {
				return yield* Effect.fail(
					new Error(
						`Local head of "${dbName}" exists but the remote head does not. This is an unexpected state.`,
					),
				);
			}
			return;
		}

		if (Option.isSome(remoteHead)) {
			return yield* joinRemote(dbName, remoteHead.value);
		}

		if (yield* createRemote(dbName)) {
			return;
		}

		const winnerHead = yield* readRemoteHead;
		if (Option.isNone(winnerHead)) {
			return yield* Effect.fail(
				new Error(`Database "${dbName}" was created concurrently but its head is missing`),
			);
		}
		yield* joinRemote(dbName, winnerHead.value);
	});