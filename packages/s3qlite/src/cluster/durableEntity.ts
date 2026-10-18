/**
 * `DurableEntity`: a self-hosted "Durable Object" on Effect cluster.
 *
 * Each entity instance (one per entity id, placed by the cluster) owns its own s3qlite database in
 * owner mode and gets it as an Effect `SqlClient`:
 *
 * - activation opens the database and claims it (restore, or reuse of the local copy, then a head
 *   compare-and-set that fences any older owner);
 * - every RPC runs with `SqlClient`, `S3qlite` and `S3qliteClient` provided;
 * - `durable` RPCs reply only after their changes are pushed; `fast` RPCs reply at once;
 * - passivation (`maxIdleTime`), shard release, rebalance and shutdown close the handler scope,
 *   which pushes the remaining changes and closes the database;
 * - a fenced instance (another runner claimed the database) dies, so the cluster rebuilds it and
 *   replays the request; the rebuild waits `fencedRestartDelay` first, so that a runner that lost
 *   its shard locks notices it before it could claim the database back.
 */
import type { S3Service } from "@effect-aws/client-s3";
import { Context, Duration, Effect, Layer, Option, Predicate, Stream } from "effect";
import type { Scope } from "effect";
import * as Entity from "effect/cluster/Entity";
import type { Sharding } from "effect/cluster/Sharding";
import type { FileSystem } from "effect/FileSystem";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as Rpc from "effect/rpc/Rpc";
import type * as RpcGroup from "effect/rpc/RpcGroup";
import * as SqlClient from "effect/sql/SqlClient";

import { initializeContexts } from "../connection";
import type { PushError } from "../engine";
import type { BucketDoesNotExistError } from "../kv/s3KV";
import * as S3qliteClient from "../sql/client";
import type { LocalKV, RemoteKV } from "../storage";
import { OwnershipLostError } from "../types";
import type { OwnerGcPolicy, OwnerSnapshotPolicy, ReplyMode } from "../types";

import { defaultDbName, fencedRestartWaitMs, replyModeOf } from "./policy";

/**
 * Rpc annotation that sets the reply mode of one RPC:
 * `Rpc.make("Get", { ... }).annotate(DurableEntity.RpcReplyMode, "fast")`.
 */
export class RpcReplyMode extends Context.Service<RpcReplyMode, ReplyMode>()(
	"@lika85456/s3qlite/cluster/RpcReplyMode",
) {}

/** Services every handler of a durable entity gets. */
export type DatabaseServices =
	| SqlClient.SqlClient
	| S3qliteClient.S3qlite
	| S3qliteClient.S3qliteClient;

export type DurableEntityOptions<Rpcs extends Rpc.Any> = {
	/** Bucket of the databases (`toLayer` only; `toLayerFromKV` takes the KV services from context). */
	readonly bucket: string;
	/** Local copies of the databases. Default `./.s3qlite/`. Keep it on a persistent disk to skip restores. */
	readonly localDirectory?: string;
	/** Reply mode of RPCs without an override. Default `durable`. */
	readonly reply?: ReplyMode;
	/** Reply mode per RPC tag; wins over the `RpcReplyMode` annotation. */
	readonly rpcReply?: { readonly [Tag in Rpcs["_tag"]]?: ReplyMode };
	/** Database name of an entity. Default `<entityType>/<entityId>`. */
	readonly dbName?: (address: {
		readonly entityType: string;
		readonly entityId: string;
	}) => string;
	/** Fast-reply coalescing window. Default 100 ms. */
	readonly pushDelayMs?: number;
	readonly snapshot?: false | OwnerSnapshotPolicy;
	readonly gc?: false | OwnerGcPolicy;
	/**
	 * How long a rebuild waits after this runner saw the entity fenced. Use at least the cluster's
	 * `shardLockRefreshInterval`. Default 10 seconds.
	 */
	readonly fencedRestartDelay?: Duration.Input;
	readonly transformResultNames?: (str: string) => string;
	readonly transformQueryNames?: (str: string) => string;
	/** Passed to `Entity.toLayer`. */
	readonly maxIdleTime?: Duration.Input;
	readonly concurrency?: number | "unbounded";
	readonly mailboxCapacity?: number | "unbounded";
	readonly disableFatalDefects?: boolean;
	readonly defectRetryPolicy?: Parameters<
		Entity.Entity<string, Rpcs>["toLayer"]
	>[1] extends infer O
		? O extends { readonly defectRetryPolicy?: infer P }
			? P
			: never
		: never;
};

export type DurableEntityLayer<Rpcs extends Rpc.Any, Handlers, RX> = Layer.Layer<
	never,
	never,
	| Exclude<
			RX,
			DatabaseServices | Scope.Scope | Entity.CurrentAddress | Entity.CurrentRunnerAddress
	  >
	| Exclude<RpcGroup.HandlersServices<Rpcs, Handlers>, DatabaseServices>
	| Rpc.ServicesClient<Rpcs>
	| Rpc.ServicesServer<Rpcs>
	| Rpc.Middleware<Rpcs>
	| Sharding
>;

const defaultFencedRestartDelay = Duration.seconds(10);

/**
 * Like `toLayer`, but the bucket and local directory come from the `LocalKV` and `RemoteKV`
 * services in context. Useful for tests and custom storage.
 */
export const toLayerFromKV = <
	Type extends string,
	Rpcs extends Rpc.Any,
	Handlers extends Entity.HandlersFrom<Rpcs>,
	RX = never,
>(
	entity: Entity.Entity<Type, Rpcs>,
	build: Effect.Effect<Handlers, never, RX>,
	options: DurableEntityOptions<Rpcs>,
): Layer.Layer<
	never,
	never,
	Layer.Services<DurableEntityLayer<Rpcs, Handlers, RX>> extends never
		? LocalKV | RemoteKV
		: LocalKV | RemoteKV | Layer.Services<DurableEntityLayer<Rpcs, Handlers, RX>>
> =>
	Layer.unwrap(
		Effect.gen(function* () {
			const kv = yield* Effect.context<LocalKV | RemoteKV>();
			// Last time this runner saw an entity fenced, by database name.
			const fencedAt = new Map<string, number>();
			const restartDelay = Duration.toMillis(
				Duration.fromInputUnsafe(options.fencedRestartDelay ?? defaultFencedRestartDelay),
			);
			const replyOf = (tag: string): ReplyMode =>
				replyModeOf(tag, {
					byTag: Option.fromNullishOr(options.rpcReply),
					annotation: Option.fromNullishOr(entity.protocol.requests.get(tag)).pipe(
						Option.flatMap((rpc) =>
							Context.getOption(
								(rpc as unknown as Rpc.AnyWithProps).annotations,
								RpcReplyMode,
							),
						),
					),
					fallback: Option.fromNullishOr(options.reply),
				});

			const wrappedBuild = Effect.gen(function* () {
				const address = yield* Entity.CurrentAddress;
				const runner = yield* Entity.CurrentRunnerAddress;
				const dbName = (options.dbName ?? defaultDbName)({
					entityType: entity.type,
					entityId: address.entityId,
				});

				const lastFenced = Option.fromNullishOr(fencedAt.get(dbName));
				if (Option.isSome(lastFenced)) {
					const remainingMs = fencedRestartWaitMs(
						lastFenced.value,
						Date.now(),
						restartDelay,
					);
					yield* Effect.logInfo("s3qlite.entity.fenced-restart-delay").pipe(
						Effect.annotateLogs({ dbName, remainingMs }),
					);
					if (remainingMs > 0) {
						yield* Effect.sleep(remainingMs);
					}
					fencedAt.delete(dbName);
				}

				// Registered before the database opens, so it runs after the final push and close.
				let closingAt = Date.now();
				yield* Effect.addFinalizer((exit) =>
					Effect.logInfo("s3qlite.entity.closed").pipe(
						Effect.annotateLogs({
							dbName,
							exit: exit._tag,
							closeMs: Date.now() - closingAt,
						}),
					),
				);

				const client = yield* S3qliteClient.make({
					dbName,
					bucket: options.bucket,
					localDirectory: options.localDirectory,
					transformQueryNames: options.transformQueryNames,
					transformResultNames: options.transformResultNames,
					syncMode: {
						type: "owner",
						// Statements reply fast; durable RPCs wait once, after the handler.
						reply: "fast",
						ownerId: `${runner.host}:${runner.port}/${crypto.randomUUID().slice(0, 8)}`,
						pushDelayMs: options.pushDelayMs,
						snapshot: options.snapshot,
						gc: options.gc,
					},
				}).pipe(Effect.provideContext(kv), Effect.provide(Reactivity.layer), Effect.orDie);

				const opened = yield* client.s3qlite.status;
				yield* Effect.logInfo("s3qlite.entity.activated").pipe(
					Effect.annotateLogs({
						dbName,
						...Option.match(opened, {
							onNone: () => ({}),
							onSome: (status) => ({ ...status.open, generation: status.generation }),
						}),
					}),
				);
				yield* Effect.addFinalizer((exit) =>
					Effect.sync(() => {
						closingAt = Date.now();
					}).pipe(
						Effect.andThen(Effect.logInfo("s3qlite.entity.closing")),
						Effect.annotateLogs({ dbName, exit: exit._tag }),
					),
				);

				const database = Context.make(S3qliteClient.S3qliteClient, client).pipe(
					Context.add(SqlClient.SqlClient, client),
					Context.add(S3qliteClient.S3qlite, client.s3qlite),
				);

				// Every failed flush ends the instance. Only a lost ownership delays the rebuild.
				const fence = (error: PushError) =>
					Effect.suspend(() => {
						if (error instanceof OwnershipLostError) {
							fencedAt.set(dbName, Date.now());
						}
						// A defect makes the cluster rebuild the entity (reopen + claim) and replay the request.
						return Effect.die(error);
					});

				const afterHandler = (mode: ReplyMode) =>
					Effect.gen(function* () {
						if (mode === "durable") {
							yield* client.s3qlite.flush.pipe(Effect.catch(fence));
						}
						const status = yield* client.s3qlite.status;
						if (Option.exists(status, (current) => current.fenced)) {
							return yield* fence(
								new OwnershipLostError({
									dbName,
									message: `Entity database "${dbName}" was claimed by another runner`,
								}),
							);
						}
					});

				const wrapValue = (value: unknown, mode: ReplyMode): unknown => {
					if (Effect.isEffect(value)) {
						return Effect.gen(function* () {
							// The reply waits even when the handler failed: it may have read or
							// written state that is not durable yet.
							const exit = yield* Effect.exit(Effect.provideContext(value, database));
							yield* afterHandler(mode);
							return yield* exit;
						});
					}
					return Stream.isStream(value) ? Stream.provideContext(value, database) : value;
				};

				const handlers = yield* build.pipe(Effect.provideContext(database));
				const wrapped: Record<string, unknown> = {};
				for (const [tag, handler] of Object.entries(
					handlers as Record<string, (request: unknown) => unknown>,
				)) {
					const mode = replyOf(tag);
					wrapped[tag] = (request: unknown) => {
						const result = handler(request);
						return Predicate.isObject(result) && Rpc.isWrapper(result)
							? Rpc.wrap({
									fork: result.fork,
									uninterruptible: result.uninterruptible,
								})(wrapValue(result.value, mode) as object)
							: wrapValue(result, mode);
					};
				}
				return wrapped as Handlers;
			});

			return entity.toLayer(wrappedBuild as Effect.Effect<Handlers, never, RX>, {
				maxIdleTime: options.maxIdleTime,
				concurrency: options.concurrency,
				mailboxCapacity: options.mailboxCapacity,
				disableFatalDefects: options.disableFatalDefects,
				defectRetryPolicy: options.defectRetryPolicy,
			});
		}),
	) as never;

/**
 * Registers `entity` with the cluster. Each entity instance gets its own owner-mode database in
 * `options.bucket` (named `<entityType>/<entityId>`), provided to `build` and to every handler as
 * `SqlClient`, `S3qlite` and `S3qliteClient`. Requires the `S3` service, a `FileSystem` and
 * `Sharding`.
 */
export const toLayer = <
	Type extends string,
	Rpcs extends Rpc.Any,
	Handlers extends Entity.HandlersFrom<Rpcs>,
	RX = never,
>(
	entity: Entity.Entity<Type, Rpcs>,
	build: Effect.Effect<Handlers, never, RX>,
	options: DurableEntityOptions<Rpcs>,
): Layer.Layer<
	never,
	BucketDoesNotExistError,
	S3Service | FileSystem | Layer.Services<DurableEntityLayer<Rpcs, Handlers, RX>>
> =>
	toLayerFromKV(entity, build, options).pipe(
		Layer.provide(
			Layer.effectContext(
				initializeContexts({
					bucket: options.bucket,
					localDirectory: options.localDirectory,
				}),
			),
		),
	) as never;