import type { Scope } from "effect";
import { Effect, Option } from "effect";
import { v4 } from "uuid";

import type { ExtractedBatch } from "./batches";
import { applyBatch, extractBatch } from "./batches";
import { getLatestChangeId } from "./cdc/extract";
import { truncate } from "./cdc/truncate";
import { ConnectionConfig, ConnectionState } from "./contexts";
import { pullFiles } from "./kv/syncFiles";
import {
	LocalKV,
	RemoteKV,
	baseKey,
	batchKey,
	dbKey,
	decodeJson,
	encodeJson,
	getJson,
	headKey,
	shmKey,
	walKey,
} from "./storage";
import type { Head, StoredHead } from "./types";

export const pull = (): Effect.Effect<
	{ newLocalBatch: Option.Option<ExtractedBatch> },
	Error,
	ConnectionConfig | ConnectionState | LocalKV | RemoteKV | Scope.Scope
> =>
	Effect.gen(function* () {
		const localKV = yield* LocalKV;
		const remoteKV = yield* RemoteKV;
		const { dbName } = yield* ConnectionConfig;
		const state = yield* ConnectionState;
		const tursoConnection = { current: yield* state.getConnection };

		yield* Effect.logDebug("s3qlite.pull.start").pipe(Effect.annotateLogs({ dbName }));

		const localHead = yield* getJson<StoredHead>(localKV, headKey(dbName)).pipe(
			Effect.flatMap(
				Option.match({
					onNone: () => Effect.die(new Error("Expected local stored head to exist")),
					onSome: Effect.succeed,
				}),
			),
		);

		const remoteHeadOption = yield* remoteKV.getIfChanged(
			headKey(dbName),
			localHead.value.remoteEtag,
		);

		// no changes detected, skip pull
		if (Option.isNone(remoteHeadOption)) {
			yield* Effect.logDebug("s3qlite.pull.skip").pipe(Effect.annotateLogs({ dbName }));
			return { newLocalBatch: Option.none() };
		}

		const remoteHead = yield* decodeJson<Head>(remoteHeadOption.value.value).pipe(
			Effect.orDieWith(
				(e) =>
					new Error(
						`Failed to decode remote head: ${String(e)}. Value to decode: ${remoteHeadOption.value.value}`,
					),
			),
		);

		const remoteBatchesToApply = remoteHead.batches.slice(localHead.value.head.batches.length);
		// Instead of rollbacking the whole database we are keeping a CoW copy of the snapshot+all applied batches up to a certain batch id.
		const lastLocalSnapshotId = localHead.value.head.snapshots.at(-1)?.id;
		const lastLocalBatchId = localHead.value.head.batches.at(-1)?.id;

		if (!lastLocalBatchId || !lastLocalSnapshotId) {
			return yield* Effect.die(
				new Error("Local head is in an invalid state, missing batches or snapshots"),
			).pipe(Effect.annotateLogs({ localHead: localHead, remoteHead }));
		}

		const baseSnapshotKey = baseKey(lastLocalSnapshotId, lastLocalBatchId);
		if (!(yield* localKV.exists(baseSnapshotKey))) {
			return yield* Effect.die(new Error("Base snapshot is missing!"));
		}

		const { newLocalBatch } = yield* Effect.scoped(
			Effect.gen(function* () {
				const workingCopyKey = `${v4()}.working-copy`;

				yield* Effect.addFinalizer(() => localKV.delete(workingCopyKey)).pipe(Effect.exit);

				const { newLocalBatch, workingConnection } = yield* Effect.all(
					{
						newLocalBatch: extractBatch(
							tursoConnection.current,
							localHead.value.lastSyncedLocalChangeId,
						),
						syncFiles: pullFiles(
							remoteBatchesToApply.map((batch) => batchKey(batch.id)),
						),
						workingConnection: localKV
							.clone(baseSnapshotKey, workingCopyKey)
							.pipe(Effect.flatMap(() => localKV.connect(workingCopyKey))),
					},
					{
						concurrency: "unbounded",
					},
				);

				// maybe it would be faster to check the sizes of the batches, and if its not bigger than a few megabytes it could just be all loaded into memory in parallel, and then applied at once, as one transaction
				for (const batch of remoteBatchesToApply) {
					yield* applyBatch(workingConnection, batch);
				}

				const nextLastRemoteAppliedChangeId = yield* getLatestChangeId(workingConnection);

				const newHead = structuredClone(remoteHead);

				const lastRemoteSnapshotId = newHead.snapshots.at(-1)?.id;
				const lastRemoteBatchId = newHead.batches.at(-1)?.id;

				if (!lastRemoteBatchId || !lastRemoteSnapshotId) {
					return yield* Effect.die(
						new Error(
							"Local head is in an invalid state, missing batches or snapshots",
						),
					).pipe(Effect.annotateLogs({ newHead }));
				}

				yield* truncate(workingConnection);
				yield* localKV.delete(walKey(workingCopyKey.split(".")[0]));

				yield* localKV.clone(
					workingCopyKey,
					baseKey(lastRemoteSnapshotId, lastRemoteBatchId),
				);

				const allPendingBatches = [
					...(localHead.value.pendingBatches ?? []),
					...(Option.isSome(newLocalBatch)
						? [
								{
									id: newLocalBatch.value.batch.id,
									lastChangeId: newLocalBatch.value.lastLocalChangeId,
								},
							]
						: []),
				];

				for (const pb of allPendingBatches) {
					yield* applyBatch(workingConnection, { id: pb.id });
				}

				yield* localKV.set(
					headKey(dbName),
					encodeJson<StoredHead>({
						head: newHead,
						remoteEtag: remoteHeadOption.value.etag,
						lastSyncedLocalChangeId: nextLastRemoteAppliedChangeId,
						...(allPendingBatches.length > 0
							? { pendingBatches: allPendingBatches }
							: {}),
					}),
				);

				const oldConnection = yield* state.getConnection;
				yield* Effect.promise(() => oldConnection.close());
				yield* localKV.delete(walKey(dbName));
				yield* localKV.delete(shmKey(dbName));
				yield* truncate(workingConnection);
				yield* localKV.clone(workingCopyKey, dbKey(dbName));

				return { newLocalBatch };
			}),
		);

		yield* state.setConnection(yield* localKV.connect(dbKey(dbName)));

		return { newLocalBatch };
	});