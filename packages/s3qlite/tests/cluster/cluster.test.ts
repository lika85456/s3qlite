import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import * as PgClient from "@effect/sql-pg/PgClient";
/**
 * Real multi-process cluster scenarios for `DurableEntity`: two or three `bun` runner processes,
 * client-only cluster processes, a local Postgres container (shard locks, messages) and the local
 * RustFS container (databases). Every scenario runs a durable-reply client and a fast-reply client
 * side by side; both entity types hash the same ids to the same shards, so one fault hits both.
 *
 * At the end every database is read straight from the bucket (after a graceful stop of the
 * survivors) and compared with the acknowledged write ids. Measured numbers are printed and
 * written to `s3qlite-tests/s3qlite-cluster-logs/results.json` in the system temp folder.
 */
import { afterAll, afterEach, describe, expect, it } from "@effect/vitest";
import { Context, Effect, Layer, Option, Redacted } from "effect";
import { SqlClient } from "effect/sql/SqlClient";
import { writeFile } from "node:fs/promises";

import { HeadStore } from "../../src/owner/headStore";
import * as S3qliteClient from "../../src/sql/client";
import { testTmpPath } from "../utils/tmp";

import { Cluster, maxAckGap, s3Layer, sleep, verify } from "./harness";
import type { Ev, Proc, Verification } from "./harness";
import { clusterPgTypes } from "./pg-types";

let cluster = Option.none<Cluster>();
const results: Record<string, unknown> = {};

afterEach(async () => {
	if (Option.isSome(cluster)) {
		await cluster.value.cleanup();
	}
	cluster = Option.none();
});

afterAll(async () => {
	await writeFile(
		testTmpPath("s3qlite-cluster-logs", "results.json"),
		JSON.stringify(results, (_key, value: unknown) => value, 2),
	);
	console.log(`cluster results\n${JSON.stringify(results, (_key, value: unknown) => value, 2)}`);
});

export const users = (count: number) =>
	Array.from({ length: count }, (_, index) => `u${index + 1}`).join(",");

export const record = (scenario: string, data: Record<string, unknown>) => {
	results[scenario] = data;
	console.log(`[${scenario}] ${JSON.stringify(data)}`);
};

const summary = (verification: Verification) => ({
	acked: verification.acked,
	stored: verification.stored,
	lost: verification.missing.length,
	duplicates: verification.duplicates,
});

const clientsDone = async (current: Cluster, names: readonly string[], timeoutMs = 90_000) => {
	for (const name of names) {
		await current.waitFor(
			(e) => e.runner === name && e.ev === "client-done",
			timeoutMs,
			`${name} done`,
		);
	}
};

export const startClients = (current: Cluster, env: Record<string, string>) => {
	current.client("durable", { REPLY: "durable", ...env });
	current.client("fast", { REPLY: "fast", ...env });
};

export const victimOf = (current: Cluster, procs: Record<string, Proc>, dbName: string) => {
	const name = Option.getOrThrowWith(
		current.ownerOf(dbName),
		() => new Error(`${dbName} has no owner`),
	);
	return { name, proc: procs[name] };
};

/**
 * Every handoff of a database after `after`: the previous owner's `closed` (final push done) must
 * come before the next owner's activation started (`activated.t - openMs`).
 */
const handoffs = (current: Cluster, dbName: string, after: number) => {
	const activations = current
		.filter((e) => e.ev === "s3qlite.entity.activated" && e.dbName === dbName)
		.sort((left, right) => left.t - right.t);
	return activations
		.filter((activation) => activation.t >= after)
		.flatMap((activation) => {
			const previous = activations.filter((e) => e.t < activation.t).at(-1);
			if (!previous || previous.runner === activation.runner) {
				return [];
			}
			const closed = Option.fromNullishOr(
				current
					.filter(
						(e: Ev) =>
							e.ev === "s3qlite.entity.closed" &&
							e.dbName === dbName &&
							e.runner === previous.runner &&
							e.t <= activation.t,
					)
					.at(-1),
			);
			const activationStart = activation.t - Number(activation.openMs ?? 0);
			return [
				{
					from: previous.runner,
					to: activation.runner,
					closedBeforeActivation: Option.exists(
						closed,
						(event) => event.t <= activationStart,
					),
					finalPushToActivationMs: Option.match(closed, {
						onNone: () => -1,
						onSome: (event) => activationStart - event.t,
					}),
					downloaded: activation.downloaded,
					openMs: activation.openMs,
				},
			];
		});
};

describe("DurableEntity on a real cluster", () => {
	it("a. graceful handoff (SIGTERM): final push before the next activation, no acknowledged loss", async () => {
		const current = await Cluster.make("a-graceful");
		cluster = Option.some(current);
		const procs = { A: await current.runner("A"), B: await current.runner("B") };
		startClients(current, { USERS: users(6), DURATION_MS: "14000" });
		await current.waitUntil(
			() => current.ackCount("durable", "u1") >= 15 && current.ackCount("fast", "u1") >= 15,
			60_000,
			"acks before SIGTERM",
		);

		const victim = victimOf(current, procs, "NotesDurable/u1");
		const killedAt = Date.now();
		victim.proc.child.kill("SIGTERM");
		await victim.proc.exited;
		const shutdownMs = Date.now() - killedAt;
		await clientsDone(current, ["durable", "fast"]);
		await current.stopAll();

		const durable = await verify(current, "NotesDurable", current.acked("durable"));
		const fast = await verify(current, "NotesFast", current.acked("fast"));
		const moved = ["NotesDurable", "NotesFast"].flatMap((type) =>
			users(6)
				.split(",")
				.flatMap((user) =>
					handoffs(current, `${type}/${user}`, killedAt).filter(
						(h) => h.from === victim.name,
					),
				),
		);
		record("a-graceful", {
			victim: victim.name,
			victimShutdownMs: shutdownMs,
			handoffs: moved.length,
			allFinalPushesBeforeActivation: moved.every((h) => h.closedBeforeActivation),
			maxAckGapMs: {
				durable: Math.max(
					...["u1"].map((u) =>
						maxAckGap(current.events, u, "a-graceful-durable", killedAt - 1000),
					),
				),
				fast: Math.max(
					...["u1"].map((u) =>
						maxAckGap(current.events, u, "a-graceful-fast", killedAt - 1000),
					),
				),
			},
			durable: summary(durable),
			fast: summary(fast),
		});
		expect(moved.length).toBeGreaterThan(0);
		expect(moved.every((h) => h.closedBeforeActivation)).toBe(true);
		expect(durable.missing).toEqual([]);
		expect(fast.missing).toEqual([]);
		expect(durable.duplicates + fast.duplicates).toBe(0);
	});

	it("b. hung runner (SIGSTOP/SIGCONT): takeover, durable replies lose nothing, stale writes fail", async () => {
		const current = await Cluster.make("b-hung");
		cluster = Option.some(current);
		const procs = { A: await current.runner("A"), B: await current.runner("B") };
		startClients(current, {
			USERS: users(4),
			DURATION_MS: "26000",
			ATTEMPT_TIMEOUT_MS: "2000",
		});
		await current.waitUntil(
			() => current.ackCount("durable", "u1") >= 15 && current.ackCount("fast", "u1") >= 15,
			60_000,
			"acks before SIGSTOP",
		);

		const victim = victimOf(current, procs, "NotesDurable/u1");
		const stoppedAt = Date.now();
		victim.proc.child.kill("SIGSTOP");
		const takeover = await current.waitFor(
			(e) =>
				e.ev === "s3qlite.entity.activated" &&
				e.dbName === "NotesDurable/u1" &&
				e.runner !== victim.name &&
				e.t > stoppedAt,
			45_000,
			"takeover of NotesDurable/u1",
		);
		await current.waitFor(
			(e) =>
				e.ev === "s3qlite.entity.activated" &&
				e.dbName === "NotesFast/u1" &&
				e.runner !== victim.name &&
				e.t > stoppedAt,
			45_000,
			"takeover of NotesFast/u1",
		);
		await sleep(2_000);
		const resumedAt = Date.now();
		victim.proc.child.kill("SIGCONT");
		await clientsDone(current, ["durable", "fast"]);
		await current.stopAll();

		const durable = await verify(current, "NotesDurable", current.acked("durable"));
		const fast = await verify(current, "NotesFast", current.acked("fast"));
		// Acks the resumed runner sent from its stale instance: after SIGCONT and before it
		// activated that entity again (the cluster may move the shard back to it later, which is
		// a new, legitimate owner).
		const lateAcks = (client: string) =>
			current.filter((e) => {
				if (
					e.runner !== client ||
					e.ev !== "ack" ||
					e.servedBy !== victim.name ||
					e.t <= resumedAt
				) {
					return false;
				}
				const type = client === "durable" ? "NotesDurable" : "NotesFast";
				const reactivated = current.find(
					(a) =>
						a.ev === "s3qlite.entity.activated" &&
						a.runner === victim.name &&
						a.dbName === `${type}/${String(e.userId)}` &&
						a.t > resumedAt,
				);
				return Option.match(reactivated, {
					onNone: () => true,
					onSome: (activation) => e.t < activation.t,
				});
			}).length;
		const fencedOnVictim = current.filter(
			(e) => e.runner === victim.name && e.ev === "s3qlite.owner.fenced" && e.t >= resumedAt,
		);
		record("b-hung", {
			victim: victim.name,
			takeoverMs: takeover.t - stoppedAt,
			lateAcksFromStaleRunner: { durable: lateAcks("durable"), fast: lateAcks("fast") },
			staleWritesFenced: {
				durable: fencedOnVictim.filter((e) => String(e.dbName).startsWith("NotesDurable"))
					.length,
				fast: fencedOnVictim.filter((e) => String(e.dbName).startsWith("NotesFast")).length,
			},
			maxAckGapMs: {
				durable: maxAckGap(current.events, "u1", "b-hung-durable", stoppedAt - 1000),
				fast: maxAckGap(current.events, "u1", "b-hung-fast", stoppedAt - 1000),
			},
			durable: summary(durable),
			fast: { ...summary(fast), lostIds: fast.missing.slice(0, 10) },
		});
		expect(durable.missing).toEqual([]);
		expect(lateAcks("durable")).toBe(0);
		expect(durable.duplicates + fast.duplicates).toBe(0);
	});

	it("c. crashed runner (SIGKILL): durable replies lose nothing, fast loss stays in the window", async () => {
		const current = await Cluster.make("c-kill");
		cluster = Option.some(current);
		const procs = { A: await current.runner("A"), B: await current.runner("B") };
		startClients(current, {
			USERS: users(4),
			DURATION_MS: "20000",
			ATTEMPT_TIMEOUT_MS: "2000",
		});
		await current.waitUntil(
			() => current.ackCount("durable", "u1") >= 15 && current.ackCount("fast", "u1") >= 15,
			60_000,
			"acks before SIGKILL",
		);

		const victim = victimOf(current, procs, "NotesDurable/u1");
		const killedAt = Date.now();
		victim.proc.child.kill("SIGKILL");
		const takeover = await current.waitFor(
			(e) =>
				e.ev === "s3qlite.entity.activated" &&
				e.dbName === "NotesDurable/u1" &&
				e.runner !== victim.name &&
				e.t > killedAt,
			45_000,
			"takeover of NotesDurable/u1",
		);
		await clientsDone(current, ["durable", "fast"]);
		await current.stopAll();

		const durable = await verify(current, "NotesDurable", current.acked("durable"));
		const fast = await verify(current, "NotesFast", current.acked("fast"));
		// How long before the kill each lost fast write was acknowledged.
		const lostAckAges = fast.missing.map((writeId) => {
			const ack = current.find((e) => e.ev === "ack" && e.writeId === writeId);
			return Option.match(ack, {
				onNone: () => Number.NaN,
				onSome: (event) => killedAt - event.t,
			});
		});
		record("c-kill", {
			victim: victim.name,
			takeoverMs: takeover.t - killedAt,
			maxAckGapMs: {
				durable: maxAckGap(current.events, "u1", "c-kill-durable", killedAt - 1000),
				fast: maxAckGap(current.events, "u1", "c-kill-fast", killedAt - 1000),
			},
			durable: summary(durable),
			fast: { ...summary(fast), lostAckAgeMs: lostAckAges },
		});
		expect(durable.missing).toEqual([]);
		// Fast replies only lose what was acknowledged within the push window before the crash
		// (coalescing delay 50 ms + one push); 1 s leaves room for a slow push on a busy machine.
		expect(lostAckAges.every((age) => age <= 1_000)).toBe(true);
		expect(durable.duplicates + fast.duplicates).toBe(0);
	});

	it("d. rebalance when a runner joins: ordered handoffs, no acknowledged loss", async () => {
		const current = await Cluster.make("d-rebalance");
		cluster = Option.some(current);
		await current.runner("A");
		startClients(current, { USERS: users(8), DURATION_MS: "16000" });
		await current.waitUntil(
			() => current.ackCount("durable", "u1") >= 15 && current.ackCount("fast", "u1") >= 15,
			60_000,
			"acks before join",
		);
		const joinedAt = Date.now();
		await current.runner("B");
		await current.waitFor(
			(e) => e.ev === "s3qlite.entity.activated" && e.runner === "B" && e.t > joinedAt,
			45_000,
			"first activation on B",
		);
		await clientsDone(current, ["durable", "fast"]);
		await current.stopAll();

		const durable = await verify(current, "NotesDurable", current.acked("durable"));
		const fast = await verify(current, "NotesFast", current.acked("fast"));
		const moved = ["NotesDurable", "NotesFast"].flatMap((type) =>
			users(8)
				.split(",")
				.flatMap((user) => handoffs(current, `${type}/${user}`, joinedAt)),
		);
		const gaps = users(8)
			.split(",")
			.map((user) => maxAckGap(current.events, user, "d-rebalance-durable", joinedAt));
		record("d-rebalance", {
			handoffs: moved.length,
			allFinalPushesBeforeActivation: moved.every((h) => h.closedBeforeActivation),
			finalPushToActivationMs: moved.map((h) => h.finalPushToActivationMs),
			maxAckGapMs: Math.max(...gaps),
			durable: summary(durable),
			fast: summary(fast),
		});
		expect(moved.length).toBeGreaterThan(0);
		expect(moved.every((h) => h.closedBeforeActivation)).toBe(true);
		expect(durable.missing).toEqual([]);
		expect(fast.missing).toEqual([]);
	});

	it("e. in-flight RPCs during a handoff plus retries with write ids: no duplicates", async () => {
		const current = await Cluster.make("e-inflight");
		cluster = Option.some(current);
		// In-flight handlers outlive the termination timeout, so shutdown interrupts them.
		const runnerEnv = { ENTITY_TERMINATION_TIMEOUT_MS: "200" };
		const procs = {
			A: await current.runner("A", runnerEnv),
			B: await current.runner("B", runnerEnv),
		};
		const clientEnv = {
			USERS: users(4),
			REPLY: "durable",
			DURATION_MS: "14000",
			DELAY_MS: "400",
			// Shorter than some handlers: the client retries the same write id while the first
			// attempt is still running.
			ATTEMPT_TIMEOUT_MS: "450",
			INTERVAL_MS: "20",
		};
		current.client("volatile", { ...clientEnv, RPC: "volatile" });
		current.client("persisted", { ...clientEnv, RPC: "persisted" });
		await current.waitUntil(
			() =>
				current.ackCount("volatile", "u1") >= 5 && current.ackCount("persisted", "u1") >= 5,
			60_000,
			"acks before SIGTERM",
		);
		const victim = victimOf(current, procs, "NotesDurable/u1");
		victim.proc.child.kill("SIGTERM");
		await victim.proc.exited;
		await clientsDone(current, ["volatile", "persisted"]);
		await current.stopAll();

		const volatile = await verify(current, "NotesDurable", current.acked("volatile"));
		const persisted = await verify(current, "NotesDurable", current.acked("persisted"));
		const attempts = (client: string) => {
			const acks = current.filter((e) => e.runner === client && e.ev === "ack");
			const failed = current.filter(
				(e) =>
					e.runner === client && (e.ev === "attempt-error" || e.ev === "attempt-timeout"),
			);
			return {
				retriedWrites: acks.filter((e) => Number(e.attempts) > 1).length,
				failedAttempts: failed.length,
			};
		};
		record("e-inflight", {
			victim: victim.name,
			volatile: {
				...summary(volatile),
				...attempts("volatile"),
			},
			persisted: {
				...summary(persisted),
				...attempts("persisted"),
			},
		});
		expect(
			attempts("volatile").retriedWrites + attempts("persisted").retriedWrites,
		).toBeGreaterThan(0);
		expect(volatile.duplicates).toBe(0);
		expect(volatile.missing).toEqual([]);
		expect(persisted.missing).toEqual([]);
	});

	it("f. restart cost and the local-cache skip", async () => {
		const current = await Cluster.make("f-restart");
		cluster = Option.some(current);
		const procs = { A: await current.runner("A", { ENTITY_MAX_IDLE_MS: "2000" }) };
		const activation = async (runner: string, after: number) =>
			current.waitFor(
				(e) =>
					e.ev === "s3qlite.entity.activated" &&
					e.dbName === "NotesDurable/u1" &&
					e.runner === runner &&
					e.t > after,
				60_000,
				`activation on ${runner}`,
			);

		// 1. First activation creates the database.
		let startedAt = Date.now();
		current.client("c1", {
			USERS: "u1",
			REPLY: "durable",
			DURATION_MS: "2000",
			INTERVAL_MS: "5",
		});
		const created = await activation("A", startedAt);
		await clientsDone(current, ["c1"]);
		// 2. Idle passivation (maxIdleTime 2 s): final push and close.
		const passivated = await current.waitFor(
			(e) =>
				e.ev === "s3qlite.entity.closed" &&
				e.dbName === "NotesDurable/u1" &&
				e.runner === "A",
			60_000,
			"idle passivation",
		);
		// 3. Reactivation on the same runner reuses the local copy.
		startedAt = Date.now();
		current.client("c2", { USERS: "u1", REPLY: "durable", DURATION_MS: "500" });
		const cached = await activation("A", startedAt);
		await clientsDone(current, ["c2"]);
		// 4. Another runner has no local copy and downloads it.
		await current.runner("B");
		procs.A.child.kill("SIGTERM");
		await procs.A.exited;
		startedAt = Date.now();
		current.client("c3", { USERS: "u1", REPLY: "durable", DURATION_MS: "500" });
		const downloaded = await activation("B", startedAt);
		await clientsDone(current, ["c3"]);
		await current.stopAll();

		const stored = await current.storedWriteIds("NotesDurable/u1");
		const acked = ["c1", "c2", "c3"].flatMap((client) => current.acked(client).get("u1") ?? []);
		record("f-restart", {
			rows: stored.length,
			idlePassivationAfterLastAckMs:
				passivated.t -
				(current.filter((e) => e.runner === "c1" && e.ev === "ack").at(-1)?.t ??
					passivated.t),
			createOpenMs: created.openMs,
			cacheHitOpenMs: cached.openMs,
			downloadOpenMs: downloaded.openMs,
			flags: [created.created, cached.downloaded, downloaded.downloaded],
		});
		expect(created.created).toBe(true);
		expect(cached.downloaded).toBe(false);
		expect(downloaded.downloaded).toBe(true);
		expect(acked.filter((id) => !stored.includes(id))).toEqual([]);
	});
});
describe("Postgres head store", () => {
	it("g. fences through Postgres and mirrors the head to the bucket", async () => {
		const current = await Cluster.make("g-pg-head-store");
		cluster = Option.some(current);
		const postgres = PgClient.layer({
			url: Redacted.make(current.baseEnv.DATABASE_URL),
			types: clusterPgTypes,
		});
		const storage = Layer.merge(fileSystemLayer, s3Layer(current.rustfs));
		const ownerLayer = (name: string) =>
			S3qliteClient.layer({
				dbName: "pg/notes",
				bucket: current.bucket,
				localDirectory: `${current.dataDir}/${name}`,
				syncMode: { type: "owner" },
			}).pipe(
				Layer.provide(HeadStore.layerSql().pipe(Layer.provide(postgres))),
				Layer.provide(storage),
			);

		const outcome = await Effect.runPromise(
			Effect.gen(function* () {
				const a = yield* Layer.build(ownerLayer("a"));
				const sqlA = Context.get(a, SqlClient);
				yield* sqlA`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`;
				yield* sqlA`INSERT INTO notes (body) VALUES ('from a')`;

				// B claims through Postgres; A's next durable write must fail.
				const b = yield* Layer.build(ownerLayer("b"));
				const stale = yield* Effect.flip(sqlA`INSERT INTO notes (body) VALUES ('stale')`);
				const rows = yield* Context.get(b, SqlClient)`SELECT body FROM notes`;

				const pg = yield* Layer.build(postgres);
				const [head] = yield* Context.get(pg, SqlClient)<{
					readonly generation: string | number;
				}>`SELECT generation FROM s3qlite_heads WHERE db_name = 'pg/notes'`;

				// A non-owner reader joins from the bucket mirror of the head.
				const reader = yield* Layer.build(
					S3qliteClient.layer({
						dbName: "pg/notes",
						bucket: current.bucket,
						localDirectory: `${current.dataDir}/reader`,
					}).pipe(Layer.provide(storage)),
				);
				const mirrored = yield* Context.get(reader, SqlClient)`SELECT body FROM notes`;
				return { stale, rows, mirrored, generation: Number(head.generation) };
			}).pipe(Effect.scoped),
		);
		record("g-pg-head-store", {
			fencedError: Option.getOrNull(
				Option.map(S3qliteClient.ownerErrorOf(outcome.stale), (error) => error._tag),
			),
			rows: outcome.rows,
			mirrored: outcome.mirrored,
			postgresGeneration: outcome.generation,
		});
		expect(Option.isSome(S3qliteClient.ownerErrorOf(outcome.stale))).toBe(true);
		expect(outcome.rows).toEqual([{ body: "from a" }]);
		expect(outcome.mirrored).toEqual([{ body: "from a" }]);
		// create, CREATE TABLE push, INSERT push, claim by B
		expect(outcome.generation).toBeGreaterThanOrEqual(4);
	});
});