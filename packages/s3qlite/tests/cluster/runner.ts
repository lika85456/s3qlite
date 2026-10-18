/**
 * Cluster runner process for the cluster suite (`vitest.cluster.config.ts`). It hosts the
 * `NotesDurable` and `NotesFast` durable entities and prints JSON event lines on stdout:
 * `{ t, runner, ev, ... }`. `s3qlite.*` log lines become events too.
 */
import { S3 } from "@effect-aws/client-s3";
import * as BunClusterHttp from "@effect/platform-bun/BunClusterHttp";
import * as BunFileSystem from "@effect/platform-bun/BunFileSystem";
import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import * as PgClient from "@effect/sql-pg/PgClient";
import { Config, Effect, Layer, Logger, Redacted, References } from "effect";
import type * as Entity from "effect/cluster/Entity";
import * as RunnerAddress from "effect/cluster/RunnerAddress";
import * as Option from "effect/Option";
import { SqlClient } from "effect/sql/SqlClient";

import * as DurableEntity from "../../src/cluster/durableEntity";
import * as Idempotency from "../../src/idempotency";

import { clusterPgTypes } from "./pg-types";
import { AppendResult, NotesDurable, NotesFast } from "./protocol";
import type { NotesRpcs } from "./protocol";

const config = Config.all({
	name: Config.String("RUNNER_NAME"),
	port: Config.Int("RUNNER_PORT"),
	databaseUrl: Config.String("DATABASE_URL"),
	s3Endpoint: Config.String("S3_ENDPOINT"),
	s3AccessKey: Config.String("S3_ACCESS_KEY"),
	s3SecretKey: Config.Redacted("S3_SECRET_KEY"),
	bucket: Config.String("S3_BUCKET"),
	localDirectory: Config.String("LOCAL_DIR"),
	shardLockExpirationMs: Config.Int("SHARD_LOCK_EXPIRATION_MS").pipe(Config.withDefault(6_000)),
	shardLockRefreshMs: Config.Int("SHARD_LOCK_REFRESH_MS").pipe(Config.withDefault(2_000)),
	entityMaxIdleMs: Config.Int("ENTITY_MAX_IDLE_MS").pipe(Config.withDefault(60_000)),
	entityTerminationTimeoutMs: Config.Int("ENTITY_TERMINATION_TIMEOUT_MS").pipe(
		Config.withDefault(5_000),
	),
	fencedRestartDelayMs: Config.Int("FENCED_RESTART_DELAY_MS").pipe(Config.withDefault(4_000)),
	pushDelayMs: Config.Int("PUSH_DELAY_MS").pipe(Config.withDefault(50)),
});

const runnerName = process.env.RUNNER_NAME ?? "?";

/** Prints one JSON event line for the harness. */
export const emit = (ev: string, data: Record<string, unknown> = {}) =>
	Effect.sync(() => {
		console.log(JSON.stringify({ t: Date.now(), runner: runnerName, ev, ...data }));
	});

/** Turns `s3qlite.*` logs and warnings into event lines; drops the rest. */
const eventLogger = Logger.map(Logger.formatStructured, (entry) => {
	const message = String(entry.message);
	if (message.startsWith("s3qlite.") || entry.level === "ERROR" || entry.level === "WARN") {
		console.log(
			JSON.stringify({
				t: Date.now(),
				runner: runnerName,
				ev: message.startsWith("s3qlite.") ? message : `log-${entry.level.toLowerCase()}`,
				level: entry.level,
				...entry.annotations,
				...(entry.cause ? { cause: entry.cause.slice(0, 600) } : {}),
			}),
		);
	}
});

const makeHandlers = <Type extends string>(entity: Entity.Entity<Type, NotesRpcs>) =>
	Effect.gen(function* () {
		const sql = yield* SqlClient;
		yield* sql`CREATE TABLE IF NOT EXISTS entries (
			seq INTEGER PRIMARY KEY,
			write_id TEXT NOT NULL,
			text TEXT NOT NULL,
			runner TEXT NOT NULL
		)`.pipe(Effect.orDie);
		yield* Idempotency.ensureTable.pipe(Effect.orDie);

		const append = (payload: {
			readonly writeId: string;
			readonly text: string;
			readonly delayMs: number;
		}) =>
			Effect.gen(function* () {
				if (payload.delayMs > 0) {
					yield* Effect.sleep(payload.delayMs);
				}
				const result = yield* Idempotency.once(
					{ writeId: payload.writeId, success: AppendResult },
					Effect.gen(function* () {
						yield* sql`INSERT INTO entries (write_id, text, runner)
							VALUES (${payload.writeId}, ${payload.text}, ${runnerName})`;
						const [row] = yield* sql<{
							readonly n: number;
						}>`SELECT count(*) AS n FROM entries`;
						return { count: row.n, appliedBy: runnerName, servedBy: runnerName };
					}),
				);
				return { ...result, servedBy: runnerName };
			}).pipe(Effect.orDie);

		return entity.of({
			Append: ({ payload }) => append(payload),
			AppendPersisted: ({ payload }) => append(payload),
			Read: () =>
				sql<{ readonly write_id: string }>`SELECT write_id FROM entries ORDER BY seq`.pipe(
					Effect.map((rows) => ({
						writeIds: rows.map((row) => row.write_id),
						servedBy: runnerName,
					})),
					Effect.orDie,
				),
		});
	});

const runtime = Layer.unwrap(
	Effect.gen(function* () {
		const settings = yield* config;
		const postgres = PgClient.layer({
			url: Redacted.make(settings.databaseUrl),
			maxConnections: 10,
			types: clusterPgTypes,
			connectTimeout: "10 seconds",
		});
		const sharding = BunClusterHttp.layer({
			transport: "http",
			storage: "sql",
			shardingConfig: {
				runnerAddress: Option.some(RunnerAddress.make("localhost", settings.port)),
				shardLockExpiration: settings.shardLockExpirationMs,
				shardLockRefreshInterval: settings.shardLockRefreshMs,
				// Advisory locks survive a hung (SIGSTOPped) runner; table locks expire.
				shardLockDisableAdvisory: true,
				refreshAssignmentsInterval: "1 second",
				entityMessagePollInterval: "1 second",
				entityTerminationTimeout: settings.entityTerminationTimeoutMs,
			},
		}).pipe(Layer.provide(postgres), Layer.orDie);
		const s3 = S3.layer({
			credentials: {
				accessKeyId: settings.s3AccessKey,
				secretAccessKey: Redacted.value(settings.s3SecretKey),
			},
			endpoint: settings.s3Endpoint,
			forcePathStyle: true,
			region: "us-east-1",
		});
		const entityOptions = {
			bucket: settings.bucket,
			localDirectory: settings.localDirectory,
			maxIdleTime: settings.entityMaxIdleMs,
			pushDelayMs: settings.pushDelayMs,
			fencedRestartDelay: settings.fencedRestartDelayMs,
			concurrency: 4,
		};
		const entities = Layer.mergeAll(
			DurableEntity.toLayer(NotesDurable, makeHandlers(NotesDurable), {
				...entityOptions,
				reply: "durable",
			}),
			DurableEntity.toLayer(NotesFast, makeHandlers(NotesFast), {
				...entityOptions,
				reply: "fast",
			}),
		).pipe(Layer.provide(Layer.merge(s3, BunFileSystem.layer)), Layer.orDie);
		const lifecycle = Layer.effectDiscard(
			Effect.gen(function* () {
				yield* emit("runner-ready", { port: settings.port });
				yield* Effect.addFinalizer(() => emit("runner-layer-closing"));
			}),
		);
		return Layer.mergeAll(entities, lifecycle).pipe(
			Layer.provide(sharding),
			Layer.provide(Logger.layer([eventLogger])),
			Layer.provide(Layer.succeed(References.MinimumLogLevel, "Info")),
		);
	}),
);

BunRuntime.runMain(Layer.launch(runtime).pipe(Effect.ensuring(emit("runner-exit"))));