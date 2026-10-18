/**
 * Global setup of the cluster suite: one local Postgres container (cluster tables and shard locks)
 * plus the shared local RustFS container of the integration suite. Never touches remote storage.
 */
import { Predicate } from "effect";
import { execFile } from "node:child_process";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";

import { setupRustfs, teardownRustfs } from "../utils/rustfs";
import { testTmpDir, testTmpPath } from "../utils/tmp";

export const clusterStateFile = testTmpPath("s3qlite-cluster-state.json");
export const pgPassword = "s3qlite";

export type ClusterState = {
	readonly pgContainer: string;
	readonly pgPort: number;
};

export const docker = (...args: string[]) =>
	new Promise<string>((resolve, reject) => {
		execFile("docker", args, (error, stdout, stderr) => {
			if (error) {
				reject(new Error(`docker ${args.join(" ")}: ${stderr || error.message}`));
				return;
			}
			resolve(stdout.trim());
		});
	});

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

const waitForPostgres = async (container: string) => {
	const startedAt = Date.now();
	while (Date.now() - startedAt < 60_000) {
		const ready = await docker(
			"exec",
			container,
			"pg_isready",
			"-U",
			"postgres",
			"-h",
			"127.0.0.1",
		)
			.then(() => true)
			.catch(() => false);
		if (ready) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 300));
	}
	throw new Error("Postgres did not become ready");
};

export default async function setup() {
	await setupRustfs();
	const pgContainer = `s3qlite-cluster-pg-${crypto.randomUUID().slice(0, 8)}`;
	const pgPort = await freePort();
	await docker(
		"run",
		"--detach",
		"--rm",
		"--name",
		pgContainer,
		"--publish",
		`${pgPort}:5432`,
		"--env",
		`POSTGRES_PASSWORD=${pgPassword}`,
		"--tmpfs",
		"/var/lib/postgresql/data:rw",
		"postgres:17-alpine",
	);
	await waitForPostgres(pgContainer);
	await mkdir(testTmpDir, { recursive: true });
	await writeFile(
		clusterStateFile,
		JSON.stringify({ pgContainer, pgPort } satisfies ClusterState),
	);

	return async () => {
		await docker("rm", "-f", pgContainer).catch(() => "");
		await rm(clusterStateFile, { force: true });
		await teardownRustfs();
	};
}