import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql/SqlClient";
import { spawn } from "node:child_process";
import type { EventEmitter } from "node:events";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { makeFileKV } from "../src/kv/fileKV";
import * as S3qliteClient from "../src/sql/client";
import { LocalKV, RemoteKV } from "../src/storage";

import { testTmpPath } from "./utils/tmp";

const fixture = fileURLToPath(new URL("./fixtures/owner-crash-writer.ts", import.meta.url));

/** Runs the writer until it acknowledged `minAcks` writes (plus a random extra delay), then SIGKILLs it. */
const runAndKill = (args: readonly string[], minAcks: number) =>
	new Promise<number>((resolve, reject) => {
		const child = spawn("bun", [fixture, ...args], { stdio: ["ignore", "pipe", "pipe"] });
		let acked = 0;
		let killScheduled = false;
		let stderr = "";
		child.stderr.on("data", (chunk: Buffer) => {
			stderr += chunk.toString();
		});
		child.stdout.on("data", (chunk: Buffer) => {
			for (const match of chunk.toString().matchAll(/ACK (\d+)/g)) {
				acked = Math.max(acked, Number(match[1]));
			}
			if (acked >= minAcks && !killScheduled) {
				killScheduled = true;
				setTimeout(() => child.kill("SIGKILL"), Math.floor(Math.random() * 15));
			}
		});
		// bun-types models ChildProcess without the EventEmitter methods.
		void once(child as unknown as EventEmitter, "exit").then(([, signal]) =>
			signal === "SIGKILL" ? resolve(acked) : reject(new Error(`writer exited: ${stderr}`)),
		);
		setTimeout(() => child.kill("SIGKILL"), 60_000);
	});

const readIds = (remoteDir: string, localDir: string, dbName: string) =>
	Effect.gen(function* () {
		const sql = yield* SqlClient;
		const rows = yield* sql<{ readonly id: number }>`SELECT id FROM rows ORDER BY id`;
		return rows.map((row) => row.id);
	}).pipe(
		Effect.provide(
			S3qliteClient.layerFromKV({ dbName, bucket: "test", syncMode: { type: "owner" } }).pipe(
				Layer.provide(Layer.effect(RemoteKV, makeFileKV(remoteDir))),
				Layer.provide(Layer.effect(LocalKV, makeFileKV(localDir))),
				Layer.provide(fileSystemLayer),
			),
		),
	);

// SIGKILL lands at random points of the push (batch upload, head CAS, local head write, CDC prune).
describe("owner mode crash recovery", () => {
	it.live.each(["durable", "fast"] as const)(
		"%s replies: SIGKILL mid-push keeps a consistent history",
		(reply) =>
			Effect.gen(function* () {
				const directory = yield* Effect.promise(() =>
					mkdtemp(testTmpPath("s3qlite-crash-")),
				);
				const remoteDir = join(directory, "remote");
				const localDir = join(directory, "local");
				const dbName = `crash-${crypto.randomUUID()}`;
				const lost: number[] = [];

				for (let round = 1; round <= 6; round += 1) {
					const acked = yield* Effect.promise(() =>
						runAndKill([remoteDir, localDir, dbName, reply], 5 + round * 3),
					);
					// Reopen on the crashed instance's own local directory (the restart path).
					const ids = yield* readIds(remoteDir, localDir, dbName);
					// The history is a gap-free prefix: no torn batch, no duplicate.
					expect(ids).toEqual(
						Array.from({ length: ids.length }, (_, index) => index + 1),
					);
					lost.push(Math.max(0, acked - ids.length));
					if (reply === "durable") {
						expect(ids.length).toBeGreaterThanOrEqual(acked);
					}
					// A fresh machine sees the same state as the restarted one.
					const fresh = yield* readIds(
						remoteDir,
						join(directory, `fresh-${round}`),
						dbName,
					);
					expect(fresh).toEqual(ids);
				}
				console.log(
					`crash-recovery ${reply}: acknowledged writes lost per kill = ${JSON.stringify(lost)}`,
				);
				yield* Effect.promise(() => rm(directory, { recursive: true, force: true }));
			}),
		120_000,
	);
});