/**
 * Owner mode: one instance is the only writer of a database. See `docs/owner-mode.md`.
 *
 * Life of an owner:
 * 1. open: restore the head (skipped when the local copy is at the remote version), then claim it
 *    by writing `generation + 1` with a compare-and-set. From then on every older holder of the
 *    head loses its next compare-and-set.
 * 2. statements run locally. A unit of work that changed the CDC log bumps `writeSeq` and wakes
 *    the pusher. Durable replies wait until `pushedSeq` covers them.
 * 3. a push round extracts the CDC rows into a batch (holding the engine permit), uploads it under
 *    a unique key, compare-and-sets the head and prunes the pushed CDC rows. A lost
 *    compare-and-set fences the instance: every later statement fails with `OwnershipLostError`.
 * 4. now and then a round also uploads a snapshot and compacts the head; garbage collection then
 *    deletes unreferenced objects older than a minimum age.
 * 5. close: a final round, then the connection closes. The local copy stays for the next open.
 */
import { Cause, Deferred, Effect, Exit, Fiber, Option, Queue, Semaphore } from "effect";
import type { Scope } from "effect";

import type { ExtractedBatch } from "../batches";
import { extractBatch } from "../batches";
import { joinRemote } from "../bootstrap";
import { getLatestChangeId } from "../cdc/extract";
import { truncate } from "../cdc/truncate";
import { ConnectionConfig } from "../contexts";
import type { S3qliteEngine } from "../engine";
import { fork } from "../fork";
import { recoverPublication } from "../publication";
import * as Reply from "../replyMode";
import {
	LocalKV,
	RemoteKV,
	batchKey,
	dbKey,
	encodeJson,
	getJson,
	headKey,
	rollbackBaseKey,
	shmKey,
	snapshotKey,
	walKey,
} from "../storage";
import {
	OwnershipLostError,
	PushFailedError,
	SourceDoesNotExistError,
	SyncModeError,
} from "../types";
import type {
	ConnectionOptions,
	Head,
	OwnerError,
	OwnerGcPolicy,
	OwnerSyncMode,
	ReplyMode,
	StoredHead,
} from "../types";

import {
	batchesAfterSnapshot,
	closeQuietly,
	compactHead,
	forkMarkerPrefix,
	headReferences,
	newObjectId,
	objectPrefix,
	pruneCdc,
	sameHead,
} from "./heads";
import { HeadStore, remoteHeadStore } from "./headStore";
import type { HeadStoreShape, VersionedHead } from "./headStore";

export { forkMarkerPrefix, objectPrefix } from "./heads";

const defaultPushDelayMs = 100;
const defaultSnapshotEveryBatches = 64;
const defaultSnapshotEveryBytes = 64 * 1024 * 1024;
const defaultGcMinAgeMs = 60 * 60 * 1000;
const claimAttempts = 5;
const finalPushAttempts = 3;
const maxRetryDelayMs = 5_000;

export type OwnerOpenStats = {
	/** `true` when the snapshot and batches were downloaded; `false` when the local copy was reused. */
	readonly downloaded: boolean;
	/** `true` when this open created the database. */
	readonly created: boolean;
	/** Time from the start of open until the claim succeeded. */
	readonly openMs: number;
};

export type OwnerStatus = {
	readonly dbName: string;
	readonly ownerId: string;
	readonly generation: number;
	/** Version of the head this owner wrote last (ETag or external generation). */
	readonly version: string;
	readonly fenced: boolean;
	/** Units of work committed locally but not pushed yet. */
	readonly pendingWrites: number;
	readonly batchesSinceSnapshot: number;
	readonly open: OwnerOpenStats;
};

export type OwnerControl = {
	readonly replyMode: ReplyMode;
	readonly status: Effect.Effect<OwnerStatus>;
	/** Pushes now (with a snapshot) and waits for it. */
	readonly snapshot: Effect.Effect<void, OwnerError>;
	/** Deletes unreferenced objects older than the GC minimum age. Returns the deleted keys. */
	readonly gc: Effect.Effect<readonly string[]>;
};

type Deps = {
	readonly dbName: string;
	readonly mode: OwnerSyncMode;
	readonly local: (typeof LocalKV)["Service"];
	readonly remote: (typeof RemoteKV)["Service"];
	readonly headStore: HeadStoreShape;
	readonly ownerId: string;
};

/** Creates a fresh database owned by this instance. Returns `None` when another creator won. */
const createOwned = (deps: Deps): Effect.Effect<Option.Option<StoredHead>, Error> =>
	Effect.gen(function* () {
		const { dbName, local, remote, headStore } = deps;
		const snapshotId = newObjectId(dbName);

		yield* Effect.all([
			local.delete(dbKey(dbName)),
			local.delete(walKey(dbName)),
			local.delete(shmKey(dbName)),
			local.delete(headKey(dbName)),
		]);
		yield* Effect.acquireUseRelease(local.open(dbKey(dbName)), truncate, closeQuietly);
		yield* local.clone(dbKey(dbName), snapshotKey(snapshotId));
		const stream = yield* local.readStream(snapshotKey(snapshotId));
		if (Option.isNone(stream)) {
			return yield* Effect.die(new Error("Snapshot not found after clone"));
		}
		yield* remote.writeStream(snapshotKey(snapshotId), stream.value);
		yield* local.delete(snapshotKey(snapshotId));

		const head: Head = {
			snapshots: [{ id: snapshotId, batchIdsApplied: [] }],
			batches: [],
			generation: 1,
			owner: deps.ownerId,
		};
		const created = yield* headStore.create(dbName, head).pipe(
			Effect.map(Option.some),
			Effect.catchTag("ConflictError", () => Effect.succeed(Option.none<string>())),
		);
		if (Option.isNone(created)) {
			yield* remote.delete(snapshotKey(snapshotId));
			return Option.none();
		}
		const stored: StoredHead = { head, remoteEtag: created.value, lastSyncedLocalChangeId: 0 };
		yield* local.set(headKey(dbName), encodeJson(stored));
		return Option.some(stored);
	});

/** Deletes files that only pulls need (owner mode never pulls). */
const cleanupJoinFiles = (deps: Deps, head: Head): Effect.Effect<void> =>
	Effect.gen(function* () {
		const keys = [
			...headReferences(head),
			...Option.toArray(rollbackBaseKey(head)).filter((key) => key.endsWith(".base")),
		];
		yield* Effect.forEach(keys, (key) => deps.local.delete(key), { discard: true });
	});

/**
 * Restores (or reuses) the local copy and claims the head. On return the local stored head holds
 * the claimed head and its version.
 */
const openAndClaim = (
	deps: Deps,
): Effect.Effect<{ readonly stored: StoredHead; readonly stats: OwnerOpenStats }, Error> =>
	Effect.gen(function* () {
		const { dbName, local, remote, headStore } = deps;
		const startedAt = Date.now();
		yield* recoverPublication(dbName).pipe(Effect.provideService(LocalKV, local));

		for (let attempt = 1; attempt <= claimAttempts; attempt += 1) {
			const remoteHead = yield* readOrImport(deps);
			if (Option.isNone(remoteHead)) {
				const created = yield* createOwned(deps);
				if (Option.isSome(created)) {
					return {
						stored: created.value,
						stats: { downloaded: false, created: true, openMs: Date.now() - startedAt },
					};
				}
				continue;
			}

			const { head, version } = remoteHead.value;
			const localHead = yield* getJson<StoredHead>(local, headKey(dbName)).pipe(Effect.orDie);
			const cacheHit =
				Option.isSome(localHead) &&
				localHead.value.value.remoteEtag === version &&
				(yield* local.exists(dbKey(dbName)));

			if (!cacheHit) {
				yield* joinRemote(dbName, { value: head, etag: version }).pipe(
					Effect.provideService(LocalKV, local),
					Effect.provideService(RemoteKV, remote),
				);
				yield* cleanupJoinFiles(deps, head);
			}

			const stored = yield* getJson<StoredHead>(local, headKey(dbName)).pipe(
				Effect.orDie,
				Effect.flatMap(
					Option.match({
						onNone: () =>
							Effect.die(new Error("Expected local stored head after restore")),
						onSome: ({ value }) => Effect.succeed(value),
					}),
				),
			);

			const claimed: Head = {
				...head,
				generation: (head.generation ?? 0) + 1,
				owner: deps.ownerId,
			};
			const claim = yield* headStore.cas(dbName, claimed, version).pipe(
				Effect.map(Option.some),
				Effect.catchTag("ConflictError", () => Effect.succeed(Option.none<string>())),
			);
			if (Option.isNone(claim)) {
				yield* Effect.logInfo("s3qlite.owner.claim-conflict").pipe(
					Effect.annotateLogs({ dbName, attempt }),
				);
				continue;
			}

			const next: StoredHead = { ...stored, head: claimed, remoteEtag: claim.value };
			yield* local.set(headKey(dbName), encodeJson(next));
			return {
				stored: next,
				stats: { downloaded: !cacheHit, created: false, openMs: Date.now() - startedAt },
			};
		}

		return yield* Effect.fail(
			new Error(`Failed to claim "${dbName}" after ${claimAttempts} attempts`),
		);
	});

/**
 * Reads the head. With an external head store, a database that so far only exists in the bucket
 * (`{dbName}.json`) is imported into the store first.
 */
const readOrImport = (deps: Deps): Effect.Effect<Option.Option<VersionedHead>> =>
	Effect.gen(function* () {
		const current = yield* deps.headStore.read(deps.dbName);
		if (Option.isSome(current) || !deps.headStore.external) {
			return current;
		}
		const bucketHead = yield* getJson<Head>(deps.remote, headKey(deps.dbName)).pipe(
			Effect.orDie,
		);
		if (Option.isNone(bucketHead)) {
			return Option.none();
		}
		yield* deps.headStore
			.create(deps.dbName, bucketHead.value.value)
			.pipe(Effect.catchTag("ConflictError", () => Effect.void));
		return yield* deps.headStore.read(deps.dbName);
	});

type UncertainRound = {
	readonly head: Head;
	readonly covered: number;
	readonly batch: Option.Option<ExtractedBatch>;
	readonly lastSyncedLocalChangeId: number;
};

type RoundResult = {
	/** `writeSeq` at extraction: every unit up to it is covered by this round. */
	readonly covered: number;
	readonly failure: Option.Option<OwnerError>;
};

export const makeOwnerEngine = (
	dbName: string,
	options: ConnectionOptions,
	mode: OwnerSyncMode,
): Effect.Effect<S3qliteEngine, Error, LocalKV | RemoteKV | Scope.Scope> =>
	Effect.gen(function* () {
		const local = yield* LocalKV;
		const remote = yield* RemoteKV;
		const headStore = Option.getOrElse(yield* Effect.serviceOption(HeadStore), () =>
			remoteHeadStore(remote),
		);
		const deps: Deps = {
			dbName,
			mode,
			local,
			remote,
			headStore,
			ownerId: mode.ownerId ?? crypto.randomUUID(),
		};
		const replyMode: ReplyMode = mode.reply ?? "durable";
		const pushDelayMs = mode.pushDelayMs ?? defaultPushDelayMs;
		const snapshotPolicy =
			mode.snapshot === false ? Option.none() : Option.some(mode.snapshot ?? {});
		const gcPolicy = Option.filter(
			Option.fromNullishOr(mode.gc),
			(gc): gc is OwnerGcPolicy => gc !== false,
		);

		const opened = yield* openAndClaim(deps);
		let stored = opened.stored;
		const connection = yield* local.open(dbKey(dbName));

		const semaphore = yield* Semaphore.make(1);
		const roundLock = yield* Semaphore.make(1);
		const wakeups = yield* Queue.unbounded<boolean>();

		let writeSeq = 0;
		let pushedSeq = 0;
		let fenced = Option.none<OwnershipLostError>();
		let closed = false;
		let bytesSinceSnapshot = 0;
		let consecutiveFailures = 0;
		let roundSignal = Deferred.makeUnsafe<RoundResult>();
		let uncertain = Option.none<UncertainRound>();

		const changeId = Effect.suspend(() => getLatestChangeId(connection)).pipe(Effect.orDie);

		// Crash recovery: rows up to the stored head are pushed, but the previous life may have
		// stopped before it pruned them.
		yield* pruneCdc(connection, stored.lastSyncedLocalChangeId);
		if ((yield* changeId) > stored.lastSyncedLocalChangeId) {
			// The local copy holds unpushed writes on top of the current remote head (same runner,
			// previous life ended without a final push). They extend the claimed head, so push them.
			writeSeq = 1;
			Queue.offerUnsafe(wakeups, false);
		}

		const closedError = new OwnershipLostError({
			dbName,
			message: `Database "${dbName}" is closed`,
		});

		const signal = (result: RoundResult) =>
			Effect.suspend(() => {
				const current = roundSignal;
				roundSignal = Deferred.makeUnsafe<RoundResult>();
				return Deferred.succeed(current, result);
			});

		const persist = (next: StoredHead, pushed: Option.Option<ExtractedBatch>) =>
			semaphore.withPermit(
				Effect.gen(function* () {
					yield* local.set(headKey(dbName), encodeJson(next));
					stored = next;
					if (Option.isSome(pushed)) {
						yield* pruneCdc(connection, pushed.value.lastLocalChangeId);
					}
				}),
			);

		/** Compare-and-set that treats "already written by us" (lost response) as success. */
		const casHead = (head: Head, version: string): Effect.Effect<string, OwnershipLostError> =>
			headStore.cas(dbName, head, version).pipe(
				Effect.catchTag("ConflictError", () =>
					headStore.read(dbName).pipe(
						Effect.flatMap((current) =>
							Option.isSome(current) && sameHead(current.value.head, head)
								? Effect.succeed(current.value.version)
								: Effect.fail(
										new OwnershipLostError({
											dbName,
											message: `Head of "${dbName}" changed: another instance claimed it`,
										}),
									),
						),
					),
				),
			);

		const uploadSnapshot = (snapshotId: string) =>
			Effect.gen(function* () {
				// The copy still holds the CDC rows of the batch that is pushed with it; a restore
				// from it must not push them again.
				yield* Effect.acquireUseRelease(
					local.open(snapshotKey(snapshotId)),
					(db) =>
						Effect.tryPromise(() => db.run("DELETE FROM turso_cdc")).pipe(
							Effect.orDie,
							Effect.andThen(truncate(db)),
						),
					closeQuietly,
				);
				const stream = yield* local.readStream(snapshotKey(snapshotId));
				if (Option.isNone(stream)) {
					return yield* Effect.die(new Error("Snapshot copy disappeared"));
				}
				yield* remote.writeStream(snapshotKey(snapshotId), stream.value);
			});

		/**
		 * The previous round failed after its compare-and-set started, so it may have landed (for
		 * example the response was lost). Adopt it when the remote head is the one we wrote, carry
		 * on when the head is unchanged, and fence otherwise.
		 */
		const reconcile = (attempt: UncertainRound) =>
			Effect.gen(function* () {
				const current = yield* headStore.read(dbName);
				if (Option.isSome(current) && sameHead(current.value.head, attempt.head)) {
					yield* persist(
						{
							head: attempt.head,
							remoteEtag: current.value.version,
							lastSyncedLocalChangeId: attempt.lastSyncedLocalChangeId,
						},
						attempt.batch,
					);
					pushedSeq = Math.max(pushedSeq, attempt.covered);
				} else if (!Option.isSome(current) || current.value.version !== stored.remoteEtag) {
					return yield* new OwnershipLostError({
						dbName,
						message: `Head of "${dbName}" changed: another instance claimed it`,
					});
				}
				uncertain = Option.none();
			});

		/** One push round. Runs under `roundLock`, uninterruptible once it started. */
		const round = (withSnapshot: boolean): Effect.Effect<RoundResult> =>
			Effect.gen(function* () {
				if (Option.isSome(fenced)) {
					return { covered: writeSeq, failure: fenced };
				}
				if (Option.isSome(uncertain)) {
					const reconciled = yield* Effect.exit(reconcile(uncertain.value));
					if (Exit.isFailure(reconciled)) {
						const lostNow = Cause.findErrorOption(reconciled.cause).pipe(
							Option.filter((error) => error instanceof OwnershipLostError),
						);
						if (Option.isSome(lostNow)) {
							fenced = Option.some(lostNow.value);
							return { covered: writeSeq, failure: fenced };
						}
						return {
							covered: writeSeq,
							failure: Option.some<OwnerError>(
								new PushFailedError({
									dbName,
									message: `Push of "${dbName}" failed; the outcome of the covered writes is unknown`,
									cause: Cause.squash(reconciled.cause),
								}),
							),
						};
					}
				}

				const extracted = yield* semaphore
					.withPermit(
						Effect.gen(function* () {
							const covered = writeSeq;
							const batch = yield* extractBatch(
								connection,
								stored.lastSyncedLocalChangeId,
								() => newObjectId(dbName),
							);
							const snapshotId = withSnapshot
								? Option.some(newObjectId(dbName))
								: Option.none<string>();
							if (Option.isSome(snapshotId)) {
								yield* truncate(connection);
								yield* local.clone(dbKey(dbName), snapshotKey(snapshotId.value));
							}
							return { covered, batch, snapshotId };
						}),
					)
					.pipe(Effect.provideService(LocalKV, local));

				const { covered, batch, snapshotId } = extracted;
				if (Option.isNone(batch) && Option.isNone(snapshotId)) {
					pushedSeq = Math.max(pushedSeq, covered);
					return { covered, failure: Option.none() };
				}
				let casStarted = false;

				const published = yield* Effect.exit(
					Effect.gen(function* () {
						if (Option.isSome(batch)) {
							const key = batchKey(batch.value.batch.id);
							const bytes = yield* local.get(key).pipe(
								Effect.flatMap(
									Option.match({
										onNone: () =>
											Effect.die(new Error(`Missing local batch ${key}`)),
										onSome: ({ value }) => Effect.succeed(value),
									}),
								),
							);
							// Keys are unique per upload, so a plain PUT never replaces another object.
							yield* remote.set(key, bytes);
						}
						if (Option.isSome(snapshotId)) {
							yield* uploadSnapshot(snapshotId.value);
						}

						const withBatch: Head = Option.match(batch, {
							onNone: () => stored.head,
							onSome: ({ batch: next }) => ({
								...stored.head,
								batches: [...stored.head.batches, next],
							}),
						});
						const nextHead = Option.match(snapshotId, {
							onNone: () => withBatch,
							onSome: (id) => compactHead(withBatch, id),
						});
						const lastSyncedLocalChangeId = Option.match(batch, {
							onNone: () => stored.lastSyncedLocalChangeId,
							onSome: (b) => b.lastLocalChangeId,
						});
						casStarted = true;
						uncertain = Option.some({
							head: nextHead,
							covered,
							batch,
							lastSyncedLocalChangeId,
						});
						const version = yield* casHead(nextHead, stored.remoteEtag);
						yield* persist(
							{ head: nextHead, remoteEtag: version, lastSyncedLocalChangeId },
							batch,
						);
						uncertain = Option.none();
						if (headStore.external) {
							// Best-effort mirror for readers of the bucket (CLI, forks, replicas).
							yield* remote.set(headKey(dbName), encodeJson(nextHead));
						}
					}),
				);

				// Local copies are only needed until the upload finished (or failed for good).
				yield* Effect.all([
					...Option.toArray(batch).map((b) => local.delete(batchKey(b.batch.id))),
					...Option.toArray(snapshotId).map((id) => local.delete(snapshotKey(id))),
				]);

				if (Exit.isSuccess(published)) {
					consecutiveFailures = 0;
					pushedSeq = Math.max(pushedSeq, covered);
					bytesSinceSnapshot = Option.isSome(snapshotId)
						? 0
						: bytesSinceSnapshot +
							Option.match(batch, { onNone: () => 0, onSome: (b) => b.bytes });
					return { covered, failure: Option.none() };
				}

				if (!casStarted) {
					uncertain = Option.none();
				}
				const lost = Cause.findErrorOption(published.cause).pipe(
					Option.filter(
						(error): error is OwnershipLostError => error instanceof OwnershipLostError,
					),
				);
				if (Option.isSome(lost)) {
					fenced = lost;
					yield* Effect.logWarning("s3qlite.owner.fenced").pipe(
						Effect.annotateLogs({ dbName, generation: stored.head.generation ?? 0 }),
					);
					return { covered, failure: lost };
				}
				consecutiveFailures += 1;
				yield* Effect.logWarning("s3qlite.owner.push-failed").pipe(
					Effect.annotateLogs({ dbName, cause: Cause.pretty(published.cause) }),
				);
				return {
					covered,
					failure: Option.some<OwnerError>(
						new PushFailedError({
							dbName,
							message: `Push of "${dbName}" failed; the outcome of the covered writes is unknown`,
							cause: Cause.squash(published.cause),
						}),
					),
				};
			}).pipe(Effect.tap(signal), Effect.uninterruptible, roundLock.withPermit);

		const snapshotDue = (): boolean =>
			Option.match(snapshotPolicy, {
				onNone: () => false,
				onSome: (policy) =>
					batchesAfterSnapshot(stored.head) >=
						(policy.everyBatches ?? defaultSnapshotEveryBatches) ||
					bytesSinceSnapshot >= (policy.everyBytes ?? defaultSnapshotEveryBytes),
			});

		const collectGarbage: Effect.Effect<readonly string[]> = Effect.gen(function* () {
			const list = remote.list;
			if (!list) {
				return [];
			}
			const minAgeMs = Option.match(gcPolicy, {
				onNone: () => defaultGcMinAgeMs,
				onSome: (policy) => policy.minAgeMs ?? defaultGcMinAgeMs,
			});
			const referenced = new Set(headReferences(stored.head));
			// Forks share objects. Follow fork markers (and forks of forks) and keep what they use.
			const visited = new Set([dbName]);
			const queue = [dbName];
			while (queue.length > 0) {
				const current = queue.shift() as string;
				for (const marker of yield* list(forkMarkerPrefix(current))) {
					const forkName = marker.key.slice(forkMarkerPrefix(current).length);
					if (visited.has(forkName)) {
						continue;
					}
					visited.add(forkName);
					queue.push(forkName);
					const forkHead = yield* getJson<Head>(remote, headKey(forkName)).pipe(
						Effect.orDie,
					);
					if (Option.isSome(forkHead)) {
						headReferences(forkHead.value.value).forEach((key) => referenced.add(key));
					}
				}
			}
			const cutoff = Date.now() - minAgeMs;
			const garbage = (yield* list(objectPrefix(dbName)))
				.filter((object) => object.lastModified < cutoff && !referenced.has(object.key))
				.map((object) => object.key);
			yield* Effect.forEach(garbage, (key) => remote.delete(key), {
				concurrency: 8,
				discard: true,
			});
			if (garbage.length > 0) {
				yield* Effect.logInfo("s3qlite.owner.gc").pipe(
					Effect.annotateLogs({ dbName, deleted: garbage.length }),
				);
			}
			return garbage;
		}).pipe(
			Effect.catchCause((cause) =>
				Effect.logWarning("s3qlite.owner.gc-failed").pipe(
					Effect.annotateLogs({ dbName, cause: Cause.pretty(cause) }),
					Effect.as([]),
				),
			),
		);

		const waitForUrgent: Effect.Effect<void> = Queue.take(wakeups).pipe(
			Effect.repeat({ until: (urgent) => urgent }),
			Effect.asVoid,
		);

		const pusher = Effect.gen(function* () {
			while (true) {
				const urgent = yield* Queue.take(wakeups);
				if (!urgent && pushDelayMs > 0) {
					yield* Effect.raceFirst(Effect.sleep(pushDelayMs), waitForUrgent);
				}
				yield* Queue.clear(wakeups);
				const result = yield* round(false);
				if (Option.isSome(fenced)) {
					return;
				}
				if (Option.isNone(result.failure)) {
					if (snapshotDue()) {
						const snapshotResult = yield* round(true);
						if (Option.isNone(snapshotResult.failure) && Option.isSome(gcPolicy)) {
							yield* collectGarbage;
						}
					}
					continue;
				}
				// Transient failure: retry later with backoff while writes are unpushed.
				yield* Effect.sleep(Math.min(maxRetryDelayMs, 100 * 2 ** consecutiveFailures));
				if (pushedSeq < writeSeq) {
					Queue.offerUnsafe(wakeups, false);
				}
			}
		});

		const pusherFiber = yield* Effect.forkScoped(pusher);

		const awaitDurable: Effect.Effect<void, OwnerError> = Effect.suspend(() => {
			const target = writeSeq;
			const loop: Effect.Effect<void, OwnerError> = Effect.suspend(() => {
				if (pushedSeq >= target) {
					return Effect.void;
				}
				if (Option.isSome(fenced)) {
					return Effect.fail(fenced.value);
				}
				if (closed) {
					return Effect.fail(closedError);
				}
				const pending = roundSignal;
				Queue.offerUnsafe(wakeups, true);
				return Deferred.await(pending).pipe(
					Effect.flatMap((result) =>
						Option.isSome(result.failure) && result.covered >= target
							? Effect.fail(result.failure.value)
							: loop,
					),
				);
			});
			return loop;
		});

		const beforeWork: Effect.Effect<Option.Option<number>, OwnershipLostError> = Effect.suspend(
			() =>
				Option.isSome(fenced)
					? Effect.fail(fenced.value)
					: closed
						? Effect.fail(closedError)
						: changeId.pipe(Effect.map(Option.some)),
		);

		const afterWork = (before: Option.Option<number>): Effect.Effect<void> =>
			Option.match(before, {
				onNone: () => Effect.void,
				onSome: (beforeId) =>
					changeId.pipe(
						Effect.flatMap((afterId) =>
							afterId === beforeId
								? Effect.void
								: Effect.sync(() => {
										writeSeq += 1;
										Queue.offerUnsafe(wakeups, false);
									}),
						),
					),
			});

		const around = <A, E, R, E2>(
			effect: Effect.Effect<A, E, R>,
			onSyncError: (error: OwnershipLostError) => E2,
		): Effect.Effect<A, E | E2, R> =>
			Effect.gen(function* () {
				const before = yield* beforeWork.pipe(Effect.mapError(onSyncError));
				const result = yield* effect;
				yield* afterWork(before);
				return result;
			});

		const replyGate: Effect.Effect<void, OwnerError> = Effect.gen(function* () {
			const override = yield* Reply.Current;
			if (Option.getOrElse(override, () => replyMode) === "durable") {
				yield* awaitDurable;
			}
		});

		let closing = false;
		const close: Effect.Effect<void> = Effect.gen(function* () {
			if (closing) {
				return;
			}
			closing = true;
			yield* Fiber.interrupt(pusherFiber);
			for (
				let attempt = 1;
				attempt <= finalPushAttempts && Option.isNone(fenced);
				attempt += 1
			) {
				const result = yield* round(false);
				if (Option.isNone(result.failure)) {
					break;
				}
			}
			yield* semaphore.withPermit(
				Effect.gen(function* () {
					closed = true;
					yield* closeQuietly(connection);
				}),
			);
			yield* signal({ covered: writeSeq, failure: Option.some(closedError) });
			yield* Effect.logDebug("s3qlite.owner.closed").pipe(
				Effect.annotateLogs({
					dbName,
					pending: writeSeq - pushedSeq,
					fenced: Option.isSome(fenced),
				}),
			);
		}).pipe(
			Effect.catchCause((cause) => Effect.logWarning("s3qlite.owner.close-failed", cause)),
		);

		yield* Effect.addFinalizer(() => close);

		const unsupported = (operation: string) =>
			Effect.fail(
				new SyncModeError({
					dbName,
					message: `${operation} is not available in owner mode: the owner is the only writer`,
				}),
			);

		const flush: Effect.Effect<void, OwnerError> = awaitDurable;

		const control: OwnerControl = {
			replyMode,
			status: Effect.sync(() => ({
				dbName,
				ownerId: deps.ownerId,
				generation: stored.head.generation ?? 0,
				version: stored.remoteEtag,
				fenced: Option.isSome(fenced),
				pendingWrites: writeSeq - pushedSeq,
				batchesSinceSnapshot: batchesAfterSnapshot(stored.head),
				open: opened.stats,
			})),
			snapshot: Effect.gen(function* () {
				const result = yield* round(true);
				if (Option.isSome(result.failure)) {
					return yield* Effect.fail(result.failure.value);
				}
			}),
			gc: collectGarbage,
		};

		yield* Effect.logDebug("s3qlite.owner.opened").pipe(
			Effect.annotateLogs({
				dbName,
				...opened.stats,
				generation: stored.head.generation ?? 0,
			}),
		);

		return {
			dbName,
			syncMode: mode,
			semaphore,
			currentConnection: () => connection,
			pull: unsupported("pull"),
			push: flush,
			sync: unsupported("sync"),
			// The fork copies the remote head, so publish local writes first.
			fork: (nextDbName) =>
				flush.pipe(
					Effect.mapError(
						(error) =>
							new SourceDoesNotExistError({
								dbName,
								message: `Cannot fork "${dbName}": ${error.message}`,
							}),
					),
					Effect.andThen(
						fork(nextDbName).pipe(
							Effect.provideService(RemoteKV, remote),
							Effect.provideService(ConnectionConfig, {
								bucket: options.bucket,
								dbName,
								livePath: dbKey(dbName),
								localDirectory: options.localDirectory ?? "./.s3qlite/",
								localHeadPath: headKey(dbName),
								options,
							}),
						),
					),
				),
			unlocked: {
				pull: unsupported("pull"),
				// Waiting for the pusher while holding the permit would deadlock.
				push: Effect.die(
					new Error("owner mode cannot push while holding the engine permit"),
				),
				sync: unsupported("sync"),
			},
			beforeWork,
			afterWork,
			around,
			replyGate,
			flush,
			owner: Option.some(control),
			close: Effect.uninterruptible(close),
		} satisfies S3qliteEngine;
	});