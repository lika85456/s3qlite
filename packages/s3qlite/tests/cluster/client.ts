/**
 * Client-only cluster process for the cluster suite. Each user gets one writer fiber that sends
 * writes with fresh write ids and retries the same id until it is acknowledged. Prints JSON event
 * lines: `ack`, `attempt-error`, `attempt-timeout`, and `client-done` with every acknowledged id.
 */
import * as BunClusterHttp from "@effect/platform-bun/BunClusterHttp";
import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import * as PgClient from "@effect/sql-pg/PgClient";
import { Config, Effect, Layer, Option, Redacted, References } from "effect";

import { clusterPgTypes } from "./pg-types";
import { entityFor } from "./protocol";

const config = Config.all({
	databaseUrl: Config.String("DATABASE_URL"),
	users: Config.String("USERS"),
	reply: Config.String("REPLY").pipe(Config.withDefault("durable")),
	rpc: Config.String("RPC").pipe(Config.withDefault("volatile")),
	durationMs: Config.Int("DURATION_MS").pipe(Config.withDefault(10_000)),
	intervalMs: Config.Int("INTERVAL_MS").pipe(Config.withDefault(50)),
	attemptTimeoutMs: Config.Int("ATTEMPT_TIMEOUT_MS").pipe(Config.withDefault(3_000)),
	delayMs: Config.Int("DELAY_MS").pipe(Config.withDefault(0)),
	runId: Config.String("RUN_ID").pipe(Config.withDefault(`r${Date.now()}`)),
	shardLockExpirationMs: Config.Int("SHARD_LOCK_EXPIRATION_MS").pipe(Config.withDefault(6_000)),
});

const clientName = process.env.CLIENT_NAME ?? "client";

/** Prints one JSON event line for the harness. */
export const emit = (ev: string, data: Record<string, unknown> = {}) =>
	Effect.sync(() => {
		console.log(JSON.stringify({ t: Date.now(), runner: clientName, ev, ...data }));
	});

type Outcome =
	| { readonly _tag: "ok"; readonly servedBy: string; readonly appliedBy: string }
	| { readonly _tag: "error"; readonly error: string }
	| { readonly _tag: "timeout" };

const program = Effect.gen(function* () {
	const settings = yield* config;
	const clientFor = yield* entityFor(settings.reply).client;
	const deadline = Date.now() + settings.durationMs;

	const writer = (userId: string) =>
		Effect.gen(function* () {
			const client = clientFor(userId);
			const acked: string[] = [];
			for (let n = 1; Date.now() < deadline; n += 1) {
				const writeId = `${settings.runId}-${userId}-${String(n).padStart(6, "0")}`;
				const sentAt = Date.now();
				for (let attempts = 1; ; attempts += 1) {
					const payload = { writeId, text: `hello ${n}`, delayMs: settings.delayMs };
					const call =
						settings.rpc === "persisted"
							? client.AppendPersisted(payload)
							: client.Append(payload);
					const outcome: Outcome = yield* call.pipe(
						Effect.timeoutOption(settings.attemptTimeoutMs),
						Effect.map(
							(result): Outcome =>
								Option.isSome(result)
									? {
											_tag: "ok",
											servedBy: result.value.servedBy,
											appliedBy: result.value.appliedBy,
										}
									: { _tag: "timeout" },
						),
						Effect.catchCause((cause) =>
							Effect.succeed<Outcome>({
								_tag: "error",
								error: String(cause).slice(0, 160),
							}),
						),
					);
					if (outcome._tag === "timeout") {
						yield* emit("attempt-timeout", { userId, writeId, attempts });
						continue;
					}
					if (outcome._tag === "error") {
						yield* emit("attempt-error", {
							userId,
							writeId,
							attempts,
							error: outcome.error,
						});
						yield* Effect.sleep(100);
						continue;
					}
					acked.push(writeId);
					yield* emit("ack", {
						userId,
						writeId,
						attempts,
						latency: Date.now() - sentAt,
						servedBy: outcome.servedBy,
						appliedBy: outcome.appliedBy,
					});
					break;
				}
				yield* Effect.sleep(settings.intervalMs);
			}
			return { userId, acked };
		});

	const results = yield* Effect.forEach(settings.users.split(","), writer, {
		concurrency: "unbounded",
	});
	yield* emit("client-done", { results });
});

const runtime = Layer.unwrap(
	Effect.gen(function* () {
		const settings = yield* config;
		const postgres = PgClient.layer({
			url: Redacted.make(settings.databaseUrl),
			maxConnections: 5,
			types: clusterPgTypes,
		});
		return BunClusterHttp.layer({
			transport: "http",
			storage: "sql",
			clientOnly: true,
			shardingConfig: {
				runnerAddress: Option.none(),
				// Clients compute assignments from runner heartbeats younger than this; it must
				// match the runners.
				shardLockExpiration: settings.shardLockExpirationMs,
				refreshAssignmentsInterval: "1 second",
				entityReplyPollInterval: "200 millis",
			},
		}).pipe(Layer.provide(postgres), Layer.orDie);
	}),
);

BunRuntime.runMain(
	program.pipe(
		Effect.provide(runtime),
		Effect.provideService(References.MinimumLogLevel, "Warn"),
	),
);