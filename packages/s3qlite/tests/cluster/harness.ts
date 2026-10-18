/**
 * Process harness of the cluster suite: spawns runner and client processes, collects their JSON
 * event lines, and verifies the final state of every database straight from the bucket.
 */
import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { Effect, Layer, Option, Predicate } from "effect";
import { SqlClient } from "effect/sql/SqlClient";
import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import type { EventEmitter } from "node:events";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import * as S3qliteClient from "../../src/sql/client";
import { testTmpPath } from "../utils/tmp";

import { clusterStateFile, docker, pgPassword } from "./setup";
import type { ClusterState } from "./setup";

const here = fileURLToPath(new URL(".", import.meta.url));
const logRoot = testTmpPath("s3qlite-cluster-logs");
const rustfsStateFile = testTmpPath("s3qlite-rustfs.json");

export type Ev = { readonly t: number; readonly runner: string; readonly ev: string } & Record<
	string,
	unknown
>;

export type Proc = {
	readonly name: string;
	readonly child: ChildProcess;
	readonly exited: Promise<void>;
};

type RustfsState = {
	readonly accessKey: string;
	readonly secretKey: string;
	readonly endpoint: string;
	readonly region: string;
};

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const freePort = () =>
	new Promise<number>((resolve, reject) => {
		const server = createServer();
		server.listen(0, "127.0.0.1", () => {
			const address = server.address();
			server.close(() =>
				typeof address === "object" && Predicate.isNotNull(address)
					? resolve(address.port)
					: reject(new Error("no port")),
			);
		});
	});

/** Settings shared by runners and clients (they must use the same shard lock expiration). */
export const lockSettings = { SHARD_LOCK_EXPIRATION_MS: "6000", SHARD_LOCK_REFRESH_MS: "2000" };

export class Cluster {
	readonly events: Ev[] = [];
	readonly procs: Proc[] = [];

	readonly scenario: string;
	readonly baseEnv: Record<string, string>;
	readonly bucket: string;
	readonly rustfs: RustfsState;
	readonly dataDir: string;

	private constructor(settings: {
		readonly scenario: string;
		readonly baseEnv: Record<string, string>;
		readonly bucket: string;
		readonly rustfs: RustfsState;
		readonly dataDir: string;
	}) {
		this.scenario = settings.scenario;
		this.baseEnv = settings.baseEnv;
		this.bucket = settings.bucket;
		this.rustfs = settings.rustfs;
		this.dataDir = settings.dataDir;
	}

	/** Fresh Postgres database, bucket and local directories for one scenario. */
	static async make(scenario: string): Promise<Cluster> {
		const pg = JSON.parse(await readFile(clusterStateFile, "utf8")) as ClusterState;
		const rustfs = JSON.parse(await readFile(rustfsStateFile, "utf8")) as RustfsState;
		const database = `s_${scenario.replace(/[^a-z0-9]/gi, "_").toLowerCase()}_${crypto.randomUUID().slice(0, 6)}`;
		await docker(
			"exec",
			pg.pgContainer,
			"psql",
			"-U",
			"postgres",
			"-c",
			`CREATE DATABASE ${database}`,
		);
		const bucket = `cluster-${crypto.randomUUID().slice(0, 8)}`;
		await Effect.runPromise(
			Effect.flatMap(Effect.service(S3), (s3) => s3.createBucket({ Bucket: bucket })).pipe(
				Effect.provide(s3Layer(rustfs)),
			),
		);
		const dataDir = testTmpPath(
			"s3qlite-cluster-data",
			`${scenario}-${crypto.randomUUID().slice(0, 6)}`,
		);
		await mkdir(dataDir, { recursive: true });
		await rm(join(logRoot, scenario), { recursive: true, force: true });
		await mkdir(join(logRoot, scenario), { recursive: true });
		return new Cluster({
			scenario,
			baseEnv: {
				DATABASE_URL: `postgres://postgres:${pgPassword}@127.0.0.1:${pg.pgPort}/${database}`,
				S3_ENDPOINT: rustfs.endpoint,
				S3_ACCESS_KEY: rustfs.accessKey,
				S3_SECRET_KEY: rustfs.secretKey,
				S3_BUCKET: bucket,
				...lockSettings,
			},
			bucket,
			rustfs,
			dataDir,
		});
	}

	private spawnProcess(name: string, script: string, env: Record<string, string>): Proc {
		const log = createWriteStream(join(logRoot, this.scenario, `${name}.log`), { flags: "a" });
		const child = spawn("bun", [join(here, script)], {
			env: { ...process.env, ...this.baseEnv, ...env },
			stdio: ["ignore", "pipe", "pipe"],
		});
		const pump = (stream: Option.Option<NodeJS.ReadableStream>) => {
			let buffered = "";
			Option.map(stream, (readable) =>
				readable.on("data", (chunk: Buffer) => {
					buffered += chunk.toString();
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) {
						log.write(`${line}\n`);
						if (line.startsWith("{")) {
							try {
								this.events.push(JSON.parse(line) as Ev);
							} catch {
								// not an event line
							}
						}
					}
				}),
			);
		};
		pump(Option.fromNullishOr(child.stdout));
		pump(Option.fromNullishOr(child.stderr));
		const proc: Proc = {
			name,
			child,
			exited: once(child as unknown as EventEmitter, "exit").then(() => Promise.resolve()),
		};
		this.procs.push(proc);
		return proc;
	}

	/** Starts a runner and waits until it is ready (runners start one at a time). */
	async runner(name: string, env: Record<string, string> = {}): Promise<Proc> {
		const port = await freePort();
		const proc = this.spawnProcess(name, "runner.ts", {
			RUNNER_NAME: name,
			RUNNER_PORT: String(port),
			LOCAL_DIR: join(this.dataDir, name),
			...env,
		});
		await this.waitFor(
			(e) => e.runner === name && e.ev === "runner-ready",
			60_000,
			`${name} ready`,
		);
		return proc;
	}

	client(name: string, env: Record<string, string>): Proc {
		return this.spawnProcess(name, "client.ts", {
			CLIENT_NAME: name,
			RUN_ID: `${this.scenario}-${name}`,
			...env,
		});
	}

	find(predicate: (event: Ev) => boolean): Option.Option<Ev> {
		return Option.fromNullishOr(this.events.find(predicate));
	}

	filter(predicate: (event: Ev) => boolean): Ev[] {
		return this.events.filter(predicate);
	}

	async waitFor(
		predicate: (event: Ev) => boolean,
		timeoutMs: number,
		label: string,
	): Promise<Ev> {
		const deadline = Date.now() + timeoutMs;
		while (Date.now() < deadline) {
			const found = this.events.find(predicate);
			if (found) {
				return found;
			}
			await sleep(25);
		}
		throw new Error(`[${this.scenario}] timed out after ${timeoutMs} ms waiting for ${label}`);
	}

	async waitUntil(condition: () => boolean, timeoutMs: number, label: string): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		while (Date.now() < deadline) {
			if (condition()) {
				return;
			}
			await sleep(25);
		}
		throw new Error(`[${this.scenario}] timed out after ${timeoutMs} ms waiting for ${label}`);
	}

	/** Acks a client received for `userId`. */
	ackCount(clientName: string, userId: string): number {
		return this.filter((e) => e.runner === clientName && e.ev === "ack" && e.userId === userId)
			.length;
	}

	/** Runner that activated `dbName` last, after `after`. */
	ownerOf(dbName: string, after = 0): Option.Option<string> {
		return Option.fromNullishOr(
			this.filter(
				(e) => e.ev === "s3qlite.entity.activated" && e.dbName === dbName && e.t >= after,
			).at(-1),
		).pipe(Option.map((event) => event.runner));
	}

	/** Graceful stop (SIGTERM) of every process that is still running. */
	async stopAll(): Promise<void> {
		for (const proc of this.procs) {
			if (isRunning(proc)) {
				proc.child.kill("SIGCONT");
				proc.child.kill("SIGTERM");
			}
		}
		await Promise.race([Promise.all(this.procs.map((proc) => proc.exited)), sleep(30_000)]);
		for (const proc of this.procs) {
			if (isRunning(proc)) {
				proc.child.kill("SIGKILL");
			}
		}
	}

	async cleanup(): Promise<void> {
		await this.stopAll();
		await rm(this.dataDir, { recursive: true, force: true });
	}

	/** Write ids stored in the database of `dbName`, read straight from the bucket. */
	async storedWriteIds(dbName: string): Promise<readonly string[]> {
		const directory = join(this.dataDir, `verify-${crypto.randomUUID().slice(0, 6)}`);
		const ids = await Effect.runPromise(
			Effect.gen(function* () {
				const sql = yield* SqlClient;
				const rows = yield* sql<{
					readonly write_id: string;
				}>`SELECT write_id FROM entries ORDER BY seq`;
				return rows.map((row) => row.write_id);
			}).pipe(
				Effect.provide(
					S3qliteClient.layer({
						dbName,
						bucket: this.bucket,
						localDirectory: directory,
					}).pipe(Layer.provide(fileSystemLayer), Layer.provide(s3Layer(this.rustfs))),
				),
			),
		);
		await rm(directory, { recursive: true, force: true });
		return ids;
	}

	/** Acknowledged write ids per user from a client's `client-done` event. */
	acked(clientName: string): ReadonlyMap<string, readonly string[]> {
		const done = this.find((e) => e.runner === clientName && e.ev === "client-done");
		const results = Option.match(done, {
			onNone: () => [],
			onSome: (event) => event.results as readonly { userId: string; acked: string[] }[],
		});
		return new Map(results.map((result) => [result.userId, result.acked]));
	}
}

/** `true` while the process has neither exited nor been killed. */
export const isRunning = (proc: Proc): boolean =>
	!Option.isSome(Option.fromNullishOr(proc.child.exitCode)) &&
	!Option.isSome(Option.fromNullishOr(proc.child.signalCode));

export const s3Layer = (rustfs: RustfsState) =>
	S3.layer({
		credentials: { accessKeyId: rustfs.accessKey, secretAccessKey: rustfs.secretKey },
		endpoint: rustfs.endpoint,
		forcePathStyle: true,
		region: rustfs.region,
	});

export type Verification = {
	readonly acked: number;
	readonly stored: number;
	readonly missing: readonly string[];
	readonly duplicates: number;
};

/** Compares the acknowledged ids of a client with what the bucket holds, per user. */
export const verify = async (
	cluster: Cluster,
	entityType: string,
	acked: ReadonlyMap<string, readonly string[]>,
): Promise<Verification> => {
	let total = 0;
	let stored = 0;
	let duplicates = 0;
	const missing: string[] = [];
	for (const [userId, ids] of acked) {
		const present = await cluster.storedWriteIds(`${entityType}/${userId}`);
		const set = new Set(present);
		total += ids.length;
		stored += present.length;
		duplicates += present.length - set.size;
		missing.push(...ids.filter((id) => !set.has(id)));
	}
	return { acked: total, stored, missing, duplicates };
};

/** Largest gap between two consecutive acks of `userId` (from one client) after `after`. */
export const maxAckGap = (
	events: readonly Ev[],
	userId: string,
	writePrefix: string,
	after = 0,
) => {
	const acks = events
		.filter(
			(e) =>
				e.ev === "ack" &&
				e.userId === userId &&
				String(e.writeId).startsWith(writePrefix) &&
				e.t >= after,
		)
		.sort((left, right) => left.t - right.t);
	let gap = 0;
	for (let index = 1; index < acks.length; index += 1) {
		gap = Math.max(gap, acks[index].t - acks[index - 1].t);
	}
	return gap;
};