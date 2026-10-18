import type { Database } from "@tursodatabase/database";
import { Context, Effect, Fiber, Option, Schedule, Semaphore } from "effect";
import type { Scope } from "effect";

import { bootstrap } from "./bootstrap";
import { getLatestChangeId } from "./cdc/extract";
import { ConnectionConfig, ConnectionState } from "./contexts";
import { fork } from "./fork";
import type { ConflictError } from "./kv/kv";
import { makeOwnerEngine } from "./owner/owner";
import type { OwnerControl } from "./owner/owner";
import { pull } from "./pull";
import { push } from "./push";
import { LocalKV, RemoteKV, dbKey, getJson, headKey } from "./storage";
import type {
	ConnectionOptions,
	ForkError,
	OwnerError,
	OwnerSyncMode,
	StoredHead,
	SyncMode,
} from "./types";

export type NormalizedSyncMode =
	| { readonly type: "manual" }
	| { readonly type: "interval"; readonly everyMs: number }
	| { readonly type: "transaction" }
	| { readonly type: "push-on-change" }
	| Readonly<OwnerSyncMode>;

export type PushError = ConflictError | OwnerError;

export type SyncError = Error | ConflictError;

/**
 * Database core shared by the `Database` wrapper (`connect`) and the Effect `SqlClient` layer.
 *
 * Every operation that touches the live connection runs while holding one permit of `semaphore`.
 * Front ends acquire the permit around user statements and call `around` inside it, so pulls,
 * pushes and statements never interleave. Sync policy (when to pull or push) is isolated in
 * `around`, the background fiber and `close`, which is where an owner mode would plug in.
 */
export type S3qliteEngine = {
	readonly dbName: string;
	readonly syncMode: NormalizedSyncMode;
	readonly semaphore: Semaphore.Semaphore;
	/** The live connection. Pull replaces it, so never cache the returned value. */
	readonly currentConnection: () => Database;
	readonly pull: Effect.Effect<void, Error>;
	readonly push: Effect.Effect<void, PushError>;
	readonly sync: Effect.Effect<void, SyncError>;
	readonly fork: (dbName: string) => Effect.Effect<void, ForkError>;
	/** Unlocked variants, for callers that already hold the permit. */
	readonly unlocked: {
		readonly pull: Effect.Effect<void, Error>;
		readonly push: Effect.Effect<void, PushError>;
		readonly sync: Effect.Effect<void, SyncError>;
	};
	/**
	 * Runs after a unit of work, after the permit is released. In owner mode with the `durable`
	 * reply mode (database default or `ReplyMode` override) it waits until the unit's changes are
	 * pushed. A no-op in the other modes.
	 */
	readonly replyGate: Effect.Effect<void, OwnerError>;
	/**
	 * Publishes every committed change and waits for it: `push` in the classic modes, a durable
	 * barrier in owner mode.
	 */
	readonly flush: Effect.Effect<void, PushError>;
	/** Owner-mode controls (status, snapshot, gc). */
	readonly owner: Option.Option<OwnerControl>;
	/**
	 * Sync mode hook that runs before one unit of user work (a statement, transaction or lease)
	 * while holding the permit. Returns the change id to hand to `afterWork`.
	 */
	readonly beforeWork: Effect.Effect<Option.Option<number>, SyncError>;
	/** Sync mode hook that runs after the unit of work, still holding the permit. */
	readonly afterWork: (before: Option.Option<number>) => Effect.Effect<void, SyncError>;
	/**
	 * `beforeWork`, `effect`, `afterWork`; sync failures are mapped with `onSyncError`. Must run
	 * while holding the permit.
	 */
	readonly around: <A, E, R, E2>(
		effect: Effect.Effect<A, E, R>,
		onSyncError: (error: SyncError) => E2,
	) => Effect.Effect<A, E | E2, R>;
	/**
	 * Stops background sync, pushes unpushed changes when `pushOnClose` is enabled and closes the
	 * connection. Idempotent; also runs when the engine's scope closes.
	 */
	readonly close: Effect.Effect<void>;
};

export const normalizeSyncMode = (
	syncMode?: SyncMode,
): Effect.Effect<NormalizedSyncMode, Error> => {
	if (!syncMode || syncMode === "manual") {
		return Effect.succeed({ type: "manual" });
	}

	if (syncMode.type === "owner") {
		const snapshot = syncMode.snapshot === false ? {} : (syncMode.snapshot ?? {});
		const gc = syncMode.gc === false ? {} : (syncMode.gc ?? {});
		const checks: readonly (readonly [string, Option.Option<number>, number])[] = [
			["pushDelayMs", Option.fromNullishOr(syncMode.pushDelayMs), 0],
			["snapshot.everyBatches", Option.fromNullishOr(snapshot.everyBatches), 1],
			["snapshot.everyBytes", Option.fromNullishOr(snapshot.everyBytes), 1],
			["gc.minAgeMs", Option.fromNullishOr(gc.minAgeMs), 0],
		];
		const invalid = checks.find(([, value, min]) =>
			Option.exists(value, (number) => !Number.isInteger(number) || number < min),
		);
		return invalid
			? Effect.fail(
					new Error(
						`syncMode.owner.${invalid[0]} must be an integer >= ${invalid[2]}, got ${String(Option.getOrNull(invalid[1]))}`,
					),
				)
			: Effect.succeed(syncMode);
	}

	if (
		syncMode.type === "interval" &&
		(!Number.isInteger(syncMode.everyMs) || syncMode.everyMs <= 0)
	) {
		return Effect.fail(
			new Error(
				`syncMode.interval.everyMs must be a positive integer, got ${syncMode.everyMs}`,
			),
		);
	}

	return Effect.succeed(syncMode);
};

export const makeEngine = (
	dbName: string,
	options: ConnectionOptions,
): Effect.Effect<S3qliteEngine, Error, LocalKV | RemoteKV | Scope.Scope> =>
	Effect.gen(function* () {
		const syncMode = yield* normalizeSyncMode(options.syncMode);
		if (syncMode.type === "owner") {
			return yield* makeOwnerEngine(dbName, options, syncMode);
		}
		const pushOnClose = options.pushOnClose ?? syncMode.type !== "manual";
		const local = yield* LocalKV;
		const remote = yield* RemoteKV;

		yield* bootstrap(dbName);

		let connection = yield* local.open(dbKey(dbName));
		let closed = false;

		const ctx = Context.make(LocalKV, local).pipe(
			Context.add(RemoteKV, remote),
			Context.add(ConnectionConfig, {
				bucket: options.bucket,
				dbName,
				livePath: dbKey(dbName),
				localDirectory: options.localDirectory ?? "./.s3qlite/",
				localHeadPath: headKey(dbName),
				options,
			}),
			Context.add(ConnectionState, {
				getConnection: Effect.sync(() => connection),
				setConnection: (next) =>
					Effect.sync(() => {
						connection = next;
					}),
			}),
		);

		const semaphore = yield* Semaphore.make(1);
		const locked = semaphore.withPermit;

		const unlockedPull = pull().pipe(Effect.asVoid, Effect.provideContext(ctx));
		const unlockedPush = push().pipe(Effect.provideContext(ctx));
		const unlockedSync = Effect.gen(function* () {
			yield* pull();
			yield* push();
		}).pipe(Effect.retry(Schedule.recurs(2)), Effect.provideContext(ctx));

		const changeId = Effect.suspend(() => getLatestChangeId(connection));

		const hasUnpushedChanges = Effect.gen(function* () {
			const stored = yield* getJson<StoredHead>(local, headKey(dbName)).pipe(Effect.orDie);
			const latest = yield* changeId;
			return Option.match(stored, {
				onNone: () => false,
				onSome: ({ value }) =>
					(value.pendingBatches?.length ?? 0) > 0 ||
					latest > value.lastSyncedLocalChangeId,
			});
		});

		const tracksChanges = syncMode.type === "transaction" || syncMode.type === "push-on-change";

		const beforeWork: Effect.Effect<Option.Option<number>, SyncError> = tracksChanges
			? Effect.gen(function* () {
					if (syncMode.type === "transaction") {
						yield* unlockedPull;
					}
					return Option.some(yield* changeId);
				})
			: Effect.succeed(Option.none());

		const afterWork = (before: Option.Option<number>): Effect.Effect<void, SyncError> =>
			Option.match(before, {
				onNone: () => Effect.void,
				onSome: (beforeId) =>
					changeId.pipe(
						Effect.flatMap((afterId) =>
							afterId > beforeId ? unlockedPush : Effect.void,
						),
					),
			});

		const around = <A, E, R, E2>(
			effect: Effect.Effect<A, E, R>,
			onSyncError: (error: SyncError) => E2,
		): Effect.Effect<A, E | E2, R> =>
			tracksChanges
				? Effect.gen(function* () {
						const before = yield* beforeWork.pipe(Effect.mapError(onSyncError));
						const result = yield* effect;
						yield* afterWork(before).pipe(Effect.mapError(onSyncError));
						return result;
					})
				: effect;

		const background =
			syncMode.type === "interval"
				? Option.some(
						yield* Effect.sleep(syncMode.everyMs).pipe(
							Effect.andThen(
								locked(unlockedSync).pipe(
									Effect.catchCause((cause) =>
										Effect.logWarning("s3qlite.interval-sync.failed").pipe(
											Effect.annotateLogs({ dbName, cause: String(cause) }),
										),
									),
								),
							),
							Effect.forever,
							Effect.forkScoped,
						),
					)
				: Option.none<Fiber.Fiber<never>>();

		const close = Effect.gen(function* () {
			if (closed) {
				return;
			}
			closed = true;

			if (Option.isSome(background)) {
				yield* Fiber.interrupt(background.value);
			}

			yield* locked(
				Effect.gen(function* () {
					if (pushOnClose && (yield* hasUnpushedChanges)) {
						yield* unlockedSync.pipe(
							Effect.catchCause((cause) =>
								Effect.logWarning("s3qlite.close.push-failed").pipe(
									Effect.annotateLogs({ dbName, cause: String(cause) }),
								),
							),
						);
					}
					yield* Effect.tryPromise(() => connection.close()).pipe(Effect.ignore);
				}),
			);
		}).pipe(Effect.catchCause((cause) => Effect.logWarning("s3qlite.close.failed", cause)));

		yield* Effect.addFinalizer(() => close);

		return {
			dbName,
			syncMode,
			semaphore,
			currentConnection: () => connection,
			pull: locked(unlockedPull),
			push: locked(unlockedPush),
			sync: locked(unlockedSync),
			fork: (nextDbName) => locked(fork(nextDbName).pipe(Effect.provideContext(ctx))),
			unlocked: { pull: unlockedPull, push: unlockedPush, sync: unlockedSync },
			replyGate: Effect.void,
			flush: locked(unlockedPush),
			owner: Option.none(),
			beforeWork,
			afterWork,
			around,
			close: Effect.uninterruptible(close),
		} satisfies S3qliteEngine;
	});