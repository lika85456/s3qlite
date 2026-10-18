/**
 * Effect `SqlClient` backed by an s3qlite database.
 *
 * The client mirrors `@effect/sql-sqlite-node`: one serialized connection, the SQLite statement
 * compiler and `BEGIN IMMEDIATE` transactions. The serializing semaphore is the s3qlite engine
 * semaphore, so statements, transactions and pulls/pushes queue behind each other. The sync mode
 * runs around every statement outside a transaction and around every connection lease (used by
 * `withTransaction`, `reserve` and streams).
 */
import type { S3Service } from "@effect-aws/client-s3";
import { Context, Effect, Layer, Option, Scope, Stream } from "effect";
import type { Cause } from "effect";
import type { FileSystem } from "effect/FileSystem";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as SqlClient from "effect/sql/SqlClient";
import type { Borrower, Connection } from "effect/sql/SqlConnection";
import { ConnectionError, SqlError, UnknownError, classifySqliteError } from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";

import { initializeContexts } from "../connection";
import { makeEngine } from "../engine";
import type { NormalizedSyncMode, PushError, S3qliteEngine, SyncError } from "../engine";
import type { BucketDoesNotExistError } from "../kv/s3KV";
import type { OwnerStatus } from "../owner/owner";
import * as Reply from "../replyMode";
import type { LocalKV, RemoteKV } from "../storage";
import { OwnershipLostError, PushFailedError, SyncModeError } from "../types";
import type { ConnectionOptions, ForkError, OwnerError } from "../types";

const ATTR_DB_SYSTEM_NAME = "db.system.name";

export const TypeId = "~@lika85456/s3qlite/S3qliteClient" as const;
export type TypeId = typeof TypeId;

export type S3qliteClientConfig = ConnectionOptions & {
	readonly dbName: string;
	readonly spanAttributes?: Record<string, unknown>;
	readonly transformResultNames?: (str: string) => string;
	readonly transformQueryNames?: (str: string) => string;
};

/**
 * Sync control for the database behind the `SqlClient`. Every operation queues behind running
 * statements and transactions.
 */
export type S3qliteService = {
	readonly dbName: string;
	readonly syncMode: NormalizedSyncMode;
	readonly pull: Effect.Effect<void, Error>;
	readonly push: Effect.Effect<void, PushError>;
	readonly sync: Effect.Effect<void, SyncError>;
	readonly fork: (dbName: string) => Effect.Effect<void, ForkError>;
	/**
	 * Publishes every committed change and waits for it. In owner mode this is the durable
	 * barrier; elsewhere it is `push`.
	 */
	readonly flush: Effect.Effect<void, PushError>;
	/** Owner mode status; `None` in the other sync modes. */
	readonly status: Effect.Effect<Option.Option<OwnerStatus>>;
	/** Owner mode: push now with a new snapshot (compacts the head). */
	readonly snapshot: Effect.Effect<void, OwnerError | SyncModeError>;
	/** Owner mode: delete unreferenced objects older than the GC minimum age. */
	readonly gc: Effect.Effect<readonly string[], SyncModeError>;
};

export class S3qlite extends Context.Service<S3qlite, S3qliteService>()(
	"@lika85456/s3qlite/S3qlite",
) {
	/** Owner mode: statements in `effect` reply only after their changes are pushed. */
	static readonly durable = Reply.durable;
	/** Owner mode: statements in `effect` reply at once; a background push follows. */
	static readonly fast = Reply.fast;
}

/**
 * The owner-mode error behind a failed statement (`OwnershipLostError` or `PushFailedError`), if
 * any. Statements fail with `SqlError`; its cause holds the owner error.
 */
export const ownerErrorOf = (error: unknown): Option.Option<OwnerError> => {
	const cause =
		error instanceof SqlError ? (error.reason as { readonly cause?: unknown }).cause : error;
	return cause instanceof OwnershipLostError || cause instanceof PushFailedError
		? Option.some(cause)
		: Option.none();
};

export type S3qliteClient = SqlClient.SqlClient & {
	readonly [TypeId]: TypeId;
	readonly config: S3qliteClientConfig;
	readonly s3qlite: S3qliteService;
	/** Not supported in sqlite */
	readonly updateValues: never;
};

export const S3qliteClient = Context.Service<S3qliteClient>("@lika85456/s3qlite/S3qliteClient");

type RunMode = "rows" | "raw" | "values";

const syncFailure = (cause: SyncError): SqlError =>
	new SqlError({
		reason: new UnknownError({
			cause,
			message: `s3qlite sync failed: ${cause.message}`,
			operation: "sync",
		}),
	});

const pushFailure = (cause: OwnerError): SqlError =>
	new SqlError({
		reason: new UnknownError({
			cause,
			message: `s3qlite durable reply failed: ${cause.message}`,
			operation: "push",
		}),
	});

const makeConnection = (engine: S3qliteEngine): Connection => {
	const run = <A>(
		sql: string,
		params: readonly unknown[],
		mode: RunMode,
	): Effect.Effect<A, SqlError> =>
		Effect.withFiber((fiber) => {
			const safeIntegers = Context.get(fiber.context, SqlClient.SafeIntegers);
			return Effect.tryPromise({
				try: async () => {
					const statement = await engine.currentConnection().prepare(sql);
					try {
						statement.safeIntegers(safeIntegers);
						if (mode === "values") {
							statement.raw(true);
						}
						// SQLite has no boolean type; bind them as 0/1 like bun:sqlite and node:sqlite.
						const values = params.map((value) =>
							typeof value === "boolean" ? Number(value) : value,
						);
						if (statement.reader) {
							return (await statement.all(...values)) as A;
						}
						const info = await statement.run(...values);
						return (mode === "raw" ? info : []) as A;
					} finally {
						statement.close();
					}
				},
				catch: (cause) =>
					new SqlError({
						reason: classifySqliteError(cause, {
							message: "Failed to execute statement",
							operation: "execute",
						}),
					}),
			});
		});

	const rows = (
		sql: string,
		params: readonly unknown[],
		transformRows: Parameters<Connection["execute"]>[2],
	) =>
		run<readonly object[]>(sql, params, "rows").pipe(
			Effect.map((result) =>
				Option.match(Option.fromUndefinedOr(transformRows), {
					onNone: () => result,
					onSome: (transform) => transform(result),
				}),
			),
		);

	return {
		execute: rows,
		executeUnprepared: rows,
		executeRaw: (sql, params) => run<unknown>(sql, params, "raw"),
		executeValues: (sql, params) => run<readonly (readonly unknown[])[]>(sql, params, "values"),
		executeValuesUnprepared: (sql, params) =>
			run<readonly (readonly unknown[])[]>(sql, params, "values"),
		executeStream: (sql, params, transformRows) =>
			Stream.fromIterableEffect(rows(sql, params, transformRows)),
	};
};

/**
 * Acquirers over the single s3qlite connection, guarded by the engine semaphore. Same behavior as
 * `SqlClient.makeSqliteAcquirers` of effect 4.0.5, kept here because effect 4.0.1 has no borrower
 * and releases the permit of its plain acquirer before the statement runs.
 *
 * SQLite can keep a transaction open after a failed COMMIT (for example a deferred foreign key
 * violation). `onCommitFailure` rolls it back; if that ROLLBACK fails too, the connection is marked
 * poisoned and every later acquisition retries the ROLLBACK before handing it out.
 */
// The Connection API takes an optional row transform as a required positional argument.
const noTransform = Option.getOrUndefined(Option.none<never>());

const makeAcquirers = (engine: S3qliteEngine, connection: Connection) => {
	let poisoned = Option.none<SqlError>();
	const rollback = Effect.suspend(() =>
		engine.currentConnection().inTransaction
			? Effect.asVoid(connection.executeUnprepared("ROLLBACK", [], noTransform))
			: Effect.void,
	);
	const poison = (cause: Cause.Cause<SqlError>) =>
		Effect.sync(() => {
			poisoned = Option.some(
				new SqlError({
					reason: new ConnectionError({
						message: "SQLite connection cannot be reused after failed COMMIT cleanup",
						operation: "rollback",
						cause,
					}),
				}),
			);
		});
	const available = Effect.suspend(() =>
		Option.isNone(poisoned)
			? Effect.succeed(connection)
			: rollback.pipe(
					Effect.tapCause(poison),
					Effect.andThen(
						Effect.sync(() => {
							poisoned = Option.none();
							return connection;
						}),
					),
				),
	);
	const transactionAcquirer: Effect.Effect<Connection, SqlError, Scope.Scope> =
		Effect.uninterruptibleMask((restore) =>
			Effect.flatMap(Effect.scope, (scope) =>
				restore(engine.semaphore.take(1)).pipe(
					Effect.andThen(Scope.addFinalizer(scope, engine.semaphore.release(1))),
					Effect.andThen(available),
				),
			),
		);
	return {
		transactionAcquirer,
		borrower: ((f) =>
			engine.semaphore.withPermit(Effect.flatMap(available, f))) satisfies Borrower,
		onCommitFailure: (_conn: Connection) => rollback.pipe(Effect.tapCause(poison)),
	};
};

/**
 * Builds a `SqlClient` over an existing engine. The engine's lifetime is managed by the caller.
 */
export const makeWithEngine = (
	engine: S3qliteEngine,
	config: S3qliteClientConfig,
): Effect.Effect<S3qliteClient, never, Reactivity.Reactivity> =>
	Effect.gen(function* () {
		const connection = makeConnection(engine);
		const base = makeAcquirers(engine, connection);

		// A lease holds the permit until its scope closes, so `afterWork` runs as a finalizer that
		// is registered after the permit release and therefore runs before it (and after COMMIT).
		const lease = base.transactionAcquirer.pipe(
			Effect.tap(() =>
				engine.beforeWork.pipe(
					Effect.mapError(syncFailure),
					Effect.tap((before) =>
						Effect.addFinalizer(() =>
							engine.afterWork(before).pipe(
								Effect.catchCause((cause) =>
									Effect.logWarning("s3qlite.sql.push-after-lease-failed").pipe(
										Effect.annotateLogs({
											dbName: engine.dbName,
											cause: String(cause),
										}),
									),
								),
							),
						),
					),
				),
			),
		);

		const ownerOnly = new SyncModeError({
			dbName: engine.dbName,
			message: "snapshot and gc are only available in owner mode",
		});

		const s3qlite: S3qliteService = {
			dbName: engine.dbName,
			syncMode: engine.syncMode,
			pull: engine.pull,
			push: engine.push,
			sync: engine.sync,
			fork: engine.fork,
			flush: engine.flush,
			status: Option.match(engine.owner, {
				onNone: () => Effect.succeed(Option.none()),
				onSome: (owner) => owner.status.pipe(Effect.map(Option.some)),
			}),
			snapshot: Option.match(engine.owner, {
				onNone: () => Effect.fail(ownerOnly),
				onSome: (owner) => owner.snapshot,
			}),
			gc: Option.match(engine.owner, {
				onNone: () => Effect.fail(ownerOnly),
				onSome: (owner) => owner.gc,
			}),
		};
		const replyGate = engine.replyGate.pipe(Effect.mapError(pushFailure));

		const client = yield* SqlClient.make({
			acquirer: lease,
			transactionAcquirer: lease,
			// The reply gate runs after the permit is released: the pusher needs the permit.
			borrower: (f) =>
				base
					.borrower((conn) => engine.around(f(conn), syncFailure))
					.pipe(Effect.tap(() => replyGate)),
			compiler: Statement.makeCompilerSqlite(config.transformQueryNames),
			onCommitFailure: base.onCommitFailure,
			releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`,
			beginTransaction: "BEGIN IMMEDIATE",
			spanAttributes: [
				...Object.entries(config.spanAttributes ?? {}),
				[ATTR_DB_SYSTEM_NAME, "sqlite"],
			],
			...(config.transformResultNames
				? { transformRows: Statement.defaultTransforms(config.transformResultNames).array }
				: {}),
		});

		// Only the outermost transaction waits for its push, after COMMIT and the permit release.
		const withTransaction = client.withTransaction;
		const withTransactionAndReply: SqlClient.SqlClient["withTransaction"] = (effect) =>
			Effect.serviceOption(client.transactionService).pipe(
				Effect.flatMap((current) =>
					Option.isSome(current)
						? withTransaction(effect)
						: withTransaction(effect).pipe(Effect.tap(() => replyGate)),
				),
			);

		return Object.assign(client, {
			[TypeId]: TypeId,
			config,
			s3qlite,
			withTransaction: withTransactionAndReply,
		}) as S3qliteClient;
	});

/**
 * Opens the database (creating or joining it remotely) and builds a `SqlClient` over it. Closing
 * the scope pushes unpushed changes when `pushOnClose` is enabled, then closes the connection.
 */
export const make = (
	config: S3qliteClientConfig,
): Effect.Effect<S3qliteClient, Error, LocalKV | RemoteKV | Reactivity.Reactivity | Scope.Scope> =>
	makeEngine(config.dbName, config).pipe(
		Effect.flatMap((engine) => makeWithEngine(engine, config)),
	);

/**
 * Like `layer`, but uses the `LocalKV` and `RemoteKV` services from the context. Useful for tests
 * and custom storage backends.
 */
export const layerFromKV = (
	config: S3qliteClientConfig,
): Layer.Layer<S3qliteClient | SqlClient.SqlClient | S3qlite, Error, LocalKV | RemoteKV> =>
	Layer.effectContext(
		make(config).pipe(
			Effect.map((client) =>
				Context.make(S3qliteClient, client).pipe(
					Context.add(SqlClient.SqlClient, client),
					Context.add(S3qlite, client.s3qlite),
				),
			),
		),
	).pipe(Layer.provide(Reactivity.layer));

/**
 * Provides `SqlClient.SqlClient` (plus `S3qlite` for sync control) backed by an s3qlite database
 * stored in `config.bucket`. Requires the `S3` service from `@effect-aws/client-s3` and a
 * `FileSystem` (for example `NodeFileSystem.layer`).
 */
export const layer = (
	config: S3qliteClientConfig,
): Layer.Layer<
	S3qliteClient | SqlClient.SqlClient | S3qlite,
	Error | BucketDoesNotExistError,
	S3Service | FileSystem
> => layerFromKV(config).pipe(Layer.provide(Layer.effectContext(initializeContexts(config))));