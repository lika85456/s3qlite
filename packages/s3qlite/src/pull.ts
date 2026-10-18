import { Effect, Option } from "effect";

import type { ExtractedBatch } from "./batches";
import { applyBatch, extractBatch } from "./batches";
import { getLatestChangeId } from "./cdc/extract";
import { truncate } from "./cdc/truncate";
import { ConnectionConfig, ConnectionState } from "./contexts";
import { pullFiles } from "./kv/syncFiles";
import { publishStaged } from "./publication";
import {
	LocalKV,
	RemoteKV,
	batchKey,
	decodeJson,
	encodeJson,
	getJson,
	headKey,
	pendingHeadKey,
	rollbackBaseKey,
	snapshotKey,
	stagedDbKey,
} from "./storage";
import type { Head, StoredHead } from "./types";

export const pull = (): Effect.Effect<
	{ newLocalBatch: Option.Option<ExtractedBatch> },
	Error,
	ConnectionConfig | ConnectionState | LocalKV | RemoteKV
> =>
	Effect.gen(function* () {
		const localKV = yield* LocalKV;
		const remoteKV = yield* RemoteKV;
		const { dbName } = yield* ConnectionConfig;
		const state = yield* ConnectionState;

		yield* Effect.logDebug("s3qlite.pull.start").pipe(Effect.annotateLogs({ dbName }));

		const localHead = yield* getJson<StoredHead>(localKV, headKey(dbName)).pipe(
			Effect.orDie,
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
			Effect.mapError(
				(e) =>
					new Error(
						`Failed to decode remote head: ${String(e)}. Value to decode: ${new TextDecoder().decode(remoteHeadOption.value.value)}`,
					),
			),
			Effect.orDie,
		);

		// Slicing by batch count is only valid when the remote log extends the local one.
		const localBatches = localHead.value.head.batches;
		const extendsLocalHistory =
			localHead.value.head.snapshots.at(-1)?.id === remoteHead.snapshots.at(-1)?.id &&
			localBatches.length <= remoteHead.batches.length &&
			localBatches.every((batch, index) => remoteHead.batches[index]?.id === batch.id);
		if (!extendsLocalHistory) {
			return yield* Effect.fail(
				new Error(
					`Remote head of "${dbName}" does not extend the local history; refusing to pull`,
				),
			);
		}

		const remoteBatchesToApply = remoteHead.batches.slice(localHead.value.head.batches.length);

		// Instead of rolling back the whole database we keep a CoW copy of the snapshot plus all
		// applied batches. Without batches the snapshot itself is that copy.
		const baseSnapshotKey = yield* Option.match(rollbackBaseKey(localHead.value.head), {
			onNone: () =>
				Effect.die(new Error("Local head is in an invalid state, missing snapshots")),
			onSome: Effect.succeed,
		});

		if (!(yield* localKV.exists(baseSnapshotKey))) {
			if (!baseSnapshotKey.endsWith(".snapshot")) {
				return yield* Effect.die(new Error("Base snapshot is missing!"));
			}
			yield* pullFiles([baseSnapshotKey]);
		}

		const newLocalBatch = yield* Effect.scoped(
			Effect.gen(function* () {
				const workingCopyKey = `${crypto.randomUUID()}.working-copy`;

				yield* Effect.addFinalizer(() =>
					Effect.all([
						localKV.delete(workingCopyKey),
						localKV.delete(`${workingCopyKey}-wal`),
						localKV.delete(`${workingCopyKey}-shm`),
					]),
				);

				const { newLocalBatch, workingConnection } = yield* Effect.all(
					{
						newLocalBatch: extractBatch(
							yield* state.getConnection,
							localHead.value.lastSyncedLocalChangeId,
						),
						syncFiles: pullFiles([
							...remoteHead.snapshots.slice(-1).map((s) => snapshotKey(s.id)),
							...remoteBatchesToApply.map((batch) => batchKey(batch.id)),
						]),
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
				yield* truncate(workingConnection);

				const newBaseKey = rollbackBaseKey(newHead);
				if (Option.isSome(newBaseKey) && !newBaseKey.value.endsWith(".snapshot")) {
					yield* localKV.clone(workingCopyKey, newBaseKey.value);
				}

				const allPendingBatches = [
					...(localHead.value.pendingBatches ?? []),
					...Option.match(newLocalBatch, {
						onNone: () => [],
						onSome: ({ batch, lastLocalChangeId }) => [
							{ id: batch.id, lastChangeId: lastLocalChangeId },
						],
					}),
				];

				for (const pb of allPendingBatches) {
					yield* applyBatch(workingConnection, { id: pb.id });
				}

				yield* truncate(workingConnection);
				yield* Effect.promise(() => workingConnection.close());

				// From here on the work is published in two atomic steps: first the complete staged
				// database, then the pending head (the commit point). See `recoverPublication`.
				yield* localKV.move(workingCopyKey, stagedDbKey(dbName));
				yield* localKV
					.set(
						pendingHeadKey(dbName),
						encodeJson<StoredHead>({
							head: newHead,
							remoteEtag: remoteHeadOption.value.etag,
							lastSyncedLocalChangeId: nextLastRemoteAppliedChangeId,
							...(allPendingBatches.length > 0
								? { pendingBatches: allPendingBatches }
								: {}),
						}),
					)
					.pipe(
						// A failed write may still have landed. If the pending head exists, the pull is
						// committed and has to be published; otherwise discard the staged copy.
						Effect.catchCause((cause) =>
							localKV
								.exists(pendingHeadKey(dbName))
								.pipe(
									Effect.flatMap((committed) =>
										committed
											? Effect.void
											: localKV
													.delete(stagedDbKey(dbName))
													.pipe(Effect.andThen(Effect.failCause(cause))),
									),
								),
						),
					);

				return newLocalBatch;
			}),
		);

		yield* publishStaged(dbName);
		yield* Effect.logDebug("s3qlite.pull.done").pipe(Effect.annotateLogs({ dbName }));

		return { newLocalBatch };
	});