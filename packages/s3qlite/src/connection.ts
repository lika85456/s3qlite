import { Context, Effect, Option, Ref, Schedule } from "effect";

import { applyBatch } from "./batches";
import { getLatestChangeId } from "./cdc/extract";
import { truncate } from "./cdc/truncate";
import { ConnectionConfig, ConnectionState } from "./contexts";
import { fork } from "./fork";
import { makeFileKV } from "./kv/fileKV";
import { makeS3KV } from "./kv/s3KV";
import { pullFiles } from "./kv/syncFiles";
import { pull } from "./pull";
import { push } from "./push";
import {
	LocalKV,
	RemoteKV,
	baseKey,
	batchKey,
	dbKey,
	encodeJson,
	getJson,
	headKey,
	snapshotKey,
} from "./storage";
import type { ConnectionOptions, Head, S3qliteDatabase, StoredHead } from "./types";
import { wrapDatabase } from "./wrapDatabase";

export const initializeContexts = (connectionOptions: ConnectionOptions) =>
	Effect.gen(function* () {
		const s3kv = yield* makeS3KV(connectionOptions.bucket);
		const filekv = yield* makeFileKV(connectionOptions.localDirectory ?? "./.s3qlite/");
		return Context.empty().pipe(Context.add(RemoteKV, s3kv), Context.add(LocalKV, filekv));
	});

export const connect = (dbName: string, options: ConnectionOptions) =>
	Effect.gen(function* () {
		const remote = yield* RemoteKV;
		const local = yield* LocalKV;

		const { localHead, remoteHead } = yield* Effect.all({
			localHead: getJson<StoredHead>(local, headKey(dbName)),
			remoteHead: getJson<Head>(remote, headKey(dbName)),
			// localDb: local.get(dbKey(dbName)),
		});

		if (Option.isNone(localHead) && Option.isNone(remoteHead)) {
			const snapshotId = crypto.randomUUID();

			// connection is scoped, will die when the effect is done
			const connection = yield* local.connect(dbKey(dbName));
			yield* truncate(connection);
			yield* Effect.promise(() => connection.close());

			const head: Head = {
				snapshots: [
					{
						id: snapshotId,
						batchIdsApplied: [],
					},
				],
				batches: [],
			};

			// TODO: this should be atomic, and the head(s) probably updated only after snapshot is written.
			const { remoteResult } = yield* Effect.all({
				remoteResult: remote.set(headKey(dbName), encodeJson(head)),
				snapshot: local.clone(dbKey(dbName), snapshotKey(snapshotId)).pipe(
					Effect.flatMap(() => local.readStream(snapshotKey(snapshotId))),
					Effect.flatMap(
						Option.match({
							onNone: () => Effect.die(new Error("Snapshot not found after clone")),
							onSome: (stream) => remote.writeStream(snapshotKey(snapshotId), stream),
						}),
					),
				),
			});

			yield* local.set(
				headKey(dbName),
				encodeJson<StoredHead>({
					head,
					remoteEtag: remoteResult.etag,
					lastSyncedLocalChangeId: 0,
				}),
			);
		}

		if (Option.isSome(remoteHead) && Option.isNone(localHead)) {
			const head = remoteHead.value.value;
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

			yield* local.clone(snapshotKey(latestSnapshot.id), dbKey(dbName));

			const db = yield* local.connect(dbKey(dbName));
			for (const batch of batchesToDownload) {
				yield* applyBatch(db, batch);
			}
			const lastSyncedLocalChangeId = yield* getLatestChangeId(db);
			yield* truncate(db);
			yield* Effect.promise(() => db.close());

			const lastBatchId = batchesToDownload.at(-1)?.id;
			if (lastBatchId) {
				yield* local.clone(dbKey(dbName), baseKey(latestSnapshot.id, lastBatchId));
			}

			yield* local.set(
				headKey(dbName),
				encodeJson<StoredHead>({
					head,
					remoteEtag: remoteHead.value.etag,
					lastSyncedLocalChangeId,
				}),
			);
		}

		if (Option.isNone(remoteHead) && Option.isSome(localHead)) {
			return yield* Effect.die(
				new Error(
					"Local head exists but remote head does not. This is an unexpected state.",
				),
			);
		}

		if (Option.isSome(remoteHead) && Option.isSome(localHead)) {
			// do nothing?
		}

		const connectionConfigValue = {
			bucket: options.bucket,
			dbName,
			livePath: dbKey(dbName),
			localDirectory: options.localDirectory ?? "./.s3qlite/",
			localHeadPath: headKey(dbName),
			options,
		};

		const connection = yield* local.connect(dbKey(dbName));

		let currentConnection = connection;

		const connectionRef = yield* Ref.make(connection);

		const connectionStateValue = {
			getConnection: Ref.get(connectionRef),
			setConnection: (conn: typeof connection) =>
				Effect.sync(() => {
					currentConnection = conn;
				}).pipe(Effect.flatMap(() => Ref.set(connectionRef, conn))),
		};

		const ctx = Context.empty().pipe(
			Context.add(ConnectionConfig, connectionConfigValue),
			Context.add(ConnectionState, connectionStateValue),
			Context.add(LocalKV, local),
			Context.add(RemoteKV, remote),
		);

		const mutex = yield* Effect.makeSemaphore(1);

		const wrapper = wrapDatabase(
			() => currentConnection,
			(call, method) => {
				if (method === "iterate" || method === "transaction") {
					return call();
				}

				return Effect.runPromise(
					mutex.withPermits(1)(
						Effect.promise(() =>
							Promise.resolve().then(call as () => Promise<unknown>),
						),
					),
				) as never;
			},
		) as unknown as S3qliteDatabase;

		wrapper.pull = (() =>
			mutex.withPermits(1)(
				pull().pipe(Effect.provide(ctx), Effect.asVoid),
			)) as typeof wrapper.pull;
		wrapper.push = () => mutex.withPermits(1)(push().pipe(Effect.provide(ctx), Effect.asVoid));
		wrapper.sync = (() =>
			mutex.withPermits(1)(
				Effect.gen(function* () {
					yield* pull();
					yield* push();
				}).pipe(Effect.retry(Schedule.recurs(2)), Effect.provide(ctx)),
			)) as typeof wrapper.sync;
		wrapper.fork = (nextDbName) =>
			mutex.withPermits(1)(fork(nextDbName).pipe(Effect.provide(ctx)));
		wrapper.checkpoint = () => Effect.die(new Error("Not implemented"));

		return wrapper;
	});