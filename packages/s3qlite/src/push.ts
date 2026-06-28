import { Effect, Option } from "effect";

import { extractBatch } from "./batches";
import { getLatestChangeId } from "./cdc/extract";
import { truncate } from "./cdc/truncate";
import { ConnectionConfig, ConnectionState } from "./contexts";
import { pushFiles } from "./kv/syncFiles";
import {
	LocalKV,
	RemoteKV,
	baseKey,
	batchKey,
	dbKey,
	encodeJson,
	getJson,
	headKey,
} from "./storage";
import type { Batch, Head, StoredHead } from "./types";

export const push = () =>
	Effect.gen(function* () {
		const localKV = yield* LocalKV;
		const remoteKV = yield* RemoteKV;
		const { dbName } = yield* ConnectionConfig;
		const state = yield* ConnectionState;
		const tursoConnection = { current: yield* state.getConnection };

		const localHead = yield* getJson<StoredHead>(localKV, headKey(dbName)).pipe(
			Effect.orDie,
			Effect.flatMap(
				Option.match({
					onNone: () => Effect.die(new Error("Expected local stored head to exist")),
					onSome: Effect.succeed,
				}),
			),
		);

		const pendingBatches: { batch: Batch; lastChangeId: number }[] = [];

		if (localHead.value.pendingBatches?.length) {
			pendingBatches.push(
				...localHead.value.pendingBatches.map((pb) => ({
					batch: { id: pb.id },
					lastChangeId: pb.lastChangeId,
				})),
			);
		}

		const extracted = yield* extractBatch(
			tursoConnection.current,
			localHead.value.lastSyncedLocalChangeId,
		);

		if (Option.isSome(extracted)) {
			pendingBatches.push({
				batch: extracted.value.batch,
				lastChangeId: extracted.value.lastLocalChangeId,
			});
		}

		if (pendingBatches.length === 0) {
			return;
		}

		yield* pushFiles(pendingBatches.map((pb) => batchKey(pb.batch.id))).pipe(Effect.orDie);

		const newHead: Head = {
			snapshots: localHead.value.head.snapshots,
			batches: localHead.value.head.batches.concat(pendingBatches.map((pb) => pb.batch)),
		};

		const result = yield* remoteKV.cas(
			headKey(dbName),
			encodeJson(newHead),
			localHead.value.remoteEtag,
		);

		const lastSyncedLocalChangeId = yield* getLatestChangeId(tursoConnection.current).pipe(
			Effect.orDie,
		);

		yield* localKV.set(
			headKey(dbName),
			encodeJson<StoredHead>({
				head: newHead,
				remoteEtag: result.etag,
				lastSyncedLocalChangeId,
			}),
		);

		const lastSnapshotId = newHead.snapshots.at(-1)?.id;
		const lastBatchId = newHead.batches.at(-1)?.id;
		if (lastSnapshotId && lastBatchId) {
			yield* truncate(tursoConnection.current);
			yield* localKV.clone(dbKey(dbName), baseKey(lastSnapshotId, lastBatchId));
		}
	});
