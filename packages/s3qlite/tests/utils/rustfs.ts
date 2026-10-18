/**
 * This is completely vibecoded, its a miracle it even works
 */
import { S3 } from "@effect-aws/client-s3";
import { layer as withLayer } from "@effect/vitest";
import { Context, Effect, Layer, Option, Schedule } from "effect";
import { execFile } from "node:child_process";
import { mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { dirname } from "node:path";

import { makeS3KV } from "../../src/kv/s3KV";

import { testTmpPath } from "./tmp";

const accessKey = "test-access-key";
const secretKey = "test-secret-key";
const image = "rustfs/rustfs:latest";
const region = "us-east-1";
const rustfsStateFile = testTmpPath("s3qlite-rustfs.json");
const rustfsStateLockDirectory = testTmpPath("s3qlite-rustfs.lock");
const rustfsStateLockOwnerFile = `${rustfsStateLockDirectory}/pid`;
const rustfsStateLockStaleMs = 2_000;
const rustfsStateLockTimeoutMs = 60_000;

let cachedState = Option.none<RustfsState>();

type RustfsState = {
	readonly accessKey: string;
	readonly containerName: string;
	readonly endpoint: string;
	readonly region: string;
	readonly secretKey: string;
};

type SharedRustfsState = RustfsState & {
	readonly owners: readonly number[];
};

export type RustfsService = {
	readonly makeBucketName: (prefix?: string) => string;
	readonly makeDbName: (prefix?: string) => string;
} & RustfsState;

export class Rustfs extends Context.Service<Rustfs, RustfsService>()(
	"@template/s3qlite/tests/Rustfs",
) {}

const run = (command: string, args: readonly string[]) =>
	Effect.callback<{ readonly code: number; readonly stdout: string }, Error>((resume) => {
		execFile(command, [...args], (error, stdout) => {
			const code = error ? (typeof error.code === "number" ? error.code : 1) : 0;
			if (error && typeof error.code !== "number") {
				resume(Effect.fail(new Error(`${command} failed to start: ${error.message}`)));
				return;
			}
			resume(Effect.succeed({ code, stdout }));
		});
	});

const docker = (...args: string[]) =>
	run("docker", args).pipe(
		Effect.flatMap(({ code, stdout }) =>
			code === 0
				? Effect.succeed(stdout.trim())
				: Effect.fail(new Error(`docker ${args.join(" ")} exited with ${code}`)),
		),
	);

const reservePort = Effect.tryPromise({
	try: () =>
		new Promise<number>((resolve, reject) => {
			const server = createServer() as ReturnType<typeof createServer> & {
				once: (event: "error", listener: (error: Error) => void) => void;
			};

			server.once("error", reject);
			server.listen(0, "127.0.0.1", () => {
				const address = server.address();

				if (!address || typeof address === "string") {
					server.close(() => reject(new Error("Unable to reserve a free port")));
					return;
				}

				server.close((error) => {
					if (error) {
						reject(error);
						return;
					}

					resolve(address.port);
				});
			});
		}),
	catch: (error) => new Error(`Failed to reserve port ${String(error)}`),
});

const isRustfsStateLockStale = async (): Promise<boolean> => {
	try {
		const owner = await readFile(rustfsStateLockOwnerFile, "utf8")
			.then(Option.some)
			.catch(() => Option.none<string>());
		if (Option.isSome(owner)) {
			const pid = Number(owner.value.trim());
			if (Number.isInteger(pid) && pid > 0) {
				return !isProcessAlive(pid);
			}
		}

		const info = await stat(rustfsStateLockDirectory);
		return Date.now() - info.mtimeMs > rustfsStateLockStaleMs;
	} catch (error) {
		if (
			error &&
			typeof error === "object" &&
			"code" in error &&
			(error.code === "ENOENT" || error.code === "ENOTDIR")
		) {
			return false;
		}

		throw error;
	}
};

const withStateLock = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | Error, R> =>
	Effect.acquireUseRelease(
		Effect.tryPromise({
			try: async () => {
				const startedAt = Date.now();

				while (true) {
					try {
						await mkdir(rustfsStateLockDirectory);
						await writeFile(rustfsStateLockOwnerFile, String(process.pid));
						return;
					} catch (error) {
						if (
							error &&
							typeof error === "object" &&
							"code" in error &&
							error.code === "EEXIST"
						) {
							if (await isRustfsStateLockStale()) {
								await rm(rustfsStateLockDirectory, {
									force: true,
									recursive: true,
								});
								continue;
							}

							if (Date.now() - startedAt > rustfsStateLockTimeoutMs) {
								throw new Error("Timed out waiting for RustFS state lock");
							}

							await new Promise((resolve) => setTimeout(resolve, 50));
							continue;
						}

						throw error;
					}
				}
			},
			catch: (error) => new Error(`Failed to acquire RustFS state lock ${String(error)}`),
		}),
		() => effect,
		() =>
			Effect.tryPromise({
				try: () => rm(rustfsStateLockDirectory, { force: true, recursive: true }),
				catch: (error) => new Error(`Failed to release RustFS state lock ${String(error)}`),
			}).pipe(Effect.catch(() => Effect.void)),
	);

const isProcessAlive = (pid: number): boolean => {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
};

const getPort = (endpoint: string): number => {
	const port = Number(new URL(endpoint).port);
	if (!Number.isInteger(port) || port <= 0) {
		throw new Error(`Invalid RustFS endpoint port: ${endpoint}`);
	}
	return port;
};

const readSharedState = Effect.tryPromise({
	try: async () => {
		const state = JSON.parse(await readFile(rustfsStateFile, "utf8")) as SharedRustfsState;
		const { owners: _owners, ...rustfsState } = state;
		cachedState = Option.some(rustfsState);
		return state;
	},
	catch: (error) => new Error(`Failed to read RustFS state ${String(error)}`),
});

const readState = readSharedState.pipe(
	Effect.map((state) => {
		const { owners: _owners, ...rustfsState } = state;
		cachedState = Option.some(rustfsState);
		return rustfsState;
	}),
);

const writeSharedState = (state: SharedRustfsState) =>
	Effect.tryPromise({
		try: async () => {
			await mkdir(dirname(rustfsStateFile), { recursive: true });
			await writeFile(rustfsStateFile, JSON.stringify(state));
			const { owners: _owners, ...rustfsState } = state;
			cachedState = Option.some(rustfsState);
		},
		catch: (error) => new Error(`Failed to write RustFS state ${String(error)}`),
	});

const removeStateFile = Effect.tryPromise({
	try: () => rm(rustfsStateFile, { force: true }),
	catch: (error) => new Error(`Failed to remove RustFS state ${String(error)}`),
}).pipe(Effect.catch(() => Effect.void));

const waitForReady = ({ name, port }: { name: string; port: number }) =>
	Effect.gen(function* () {
		const ready = yield* run("curl", ["-fsS", `http://127.0.0.1:${port}/health/ready`]);

		if (ready.code === 0) {
			return;
		}

		const status = yield* docker("inspect", "--format", "{{.State.Status}}", name).pipe(
			Effect.catch(() => Effect.succeed("missing")),
		);

		if (status === "exited" || status === "dead" || status === "missing") {
			const logs = yield* docker("logs", name).pipe(Effect.catch(() => Effect.succeed("")));
			return yield* Effect.fail(
				new Error(`RustFS exited before becoming ready${logs === "" ? "" : `\n${logs}`}`),
			);
		}

		return yield* Effect.fail(new Error(`RustFS is not ready on 127.0.0.1:${port}`));
	}).pipe(
		Effect.retry(Schedule.spaced("500 millis")),
		Effect.timeoutOrElse({
			duration: "45 seconds",
			orElse: () =>
				Effect.fail(new Error(`RustFS did not become ready on 127.0.0.1:${port} in time`)),
		}),
	);
const loadState = readState.pipe(
	Effect.catch((error) =>
		Option.match(cachedState, {
			onNone: () => Effect.fail(error),
			onSome: Effect.succeed,
		}),
	),
);

export const RustfsLive = Layer.unwrap(
	loadState.pipe(
		Effect.map((state) => {
			const service = {
				...state,
				makeBucketName: (prefix = "bucket") =>
					`${prefix}-${crypto.randomUUID().slice(0, 8)}`,
				makeDbName: (prefix = "db") => `${prefix}-${crypto.randomUUID().slice(0, 8)}`,
			} satisfies RustfsService;

			return Layer.merge(
				Layer.succeed(Rustfs, service),
				S3.layer({
					credentials: {
						accessKeyId: state.accessKey,
						secretAccessKey: state.secretKey,
					},
					endpoint: state.endpoint,
					forcePathStyle: true,
					region: state.region,
				}),
			);
		}),
	),
);

export const rustfs = withLayer(RustfsLive, { timeout: "60 seconds" });

export const makeRustfsRemoteKV = (prefix = "bucket") =>
	Effect.gen(function* () {
		const s3 = yield* S3;
		const { makeBucketName } = yield* Rustfs;
		const bucket = makeBucketName(prefix);

		yield* s3.createBucket({ Bucket: bucket });
		return yield* makeS3KV(bucket);
	});

export const setupRustfs = async (): Promise<void> => {
	await Effect.runPromise(
		withStateLock(
			Effect.gen(function* () {
				const sharedState = yield* readSharedState.pipe(Effect.option);

				if (sharedState._tag === "Some") {
					const state = sharedState.value;
					if (
						yield* waitForReady({
							name: state.containerName,
							port: getPort(state.endpoint),
						}).pipe(
							Effect.as(true),
							Effect.catch(() => Effect.succeed(false)),
						)
					) {
						const liveOwners = state.owners.filter(isProcessAlive);
						yield* writeSharedState({
							...state,
							owners: liveOwners.includes(process.pid)
								? liveOwners
								: [...liveOwners, process.pid],
						});
						return;
					}

					yield* docker("rm", "-f", state.containerName).pipe(
						Effect.catch(() => Effect.void),
						Effect.asVoid,
					);
					yield* removeStateFile;
				}

				const containerName = `rustfs-test-${crypto.randomUUID()}`;
				const port = yield* reservePort;

				yield* docker(
					"run",
					"--detach",
					"--rm",
					"--name",
					containerName,
					"--publish",
					`${port}:9000`,
					"--tmpfs",
					"/data:rw,size=256m,uid=10001,gid=10001,mode=1777",
					"--tmpfs",
					"/logs:rw,size=64m,uid=10001,gid=10001,mode=1777",
					"--tmpfs",
					"/tmp:rw,size=64m,uid=10001,gid=10001,mode=1777",
					"--env",
					"RUSTFS_ADDRESS=0.0.0.0:9000",
					"--env",
					"RUSTFS_CONSOLE_ADDRESS=0.0.0.0:9001",
					"--env",
					"RUSTFS_VOLUMES=/data/rustfs{0..3}",
					"--env",
					"RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true",
					"--env",
					`RUSTFS_ACCESS_KEY=${accessKey}`,
					"--env",
					`RUSTFS_SECRET_KEY=${secretKey}`,
					image,
				).pipe(
					Effect.tapError(() =>
						docker("rm", "-f", containerName).pipe(
							Effect.catch(() => Effect.void),
							Effect.asVoid,
						),
					),
				);

				yield* waitForReady({ name: containerName, port });
				yield* writeSharedState({
					accessKey,
					containerName,
					endpoint: `http://127.0.0.1:${port}`,
					owners: [process.pid],
					region,
					secretKey,
				});
			}),
		),
	);
};

export const teardownRustfs = async (): Promise<void> => {
	await Effect.runPromise(
		withStateLock(
			Effect.gen(function* () {
				const sharedState = yield* readSharedState.pipe(Effect.option);
				if (sharedState._tag === "None") {
					cachedState = Option.none();
					return;
				}

				const state = sharedState.value;
				const nextOwners = state.owners
					.filter((owner) => owner !== process.pid)
					.filter(isProcessAlive);

				if (nextOwners.length > 0) {
					yield* writeSharedState({
						...state,
						owners: nextOwners,
					});
					return;
				}

				yield* docker("rm", "-f", state.containerName).pipe(
					Effect.catch(() => Effect.void),
					Effect.asVoid,
				);
				yield* removeStateFile;
				cachedState = Option.none();
			}),
		),
	);
};

export default async function globalSetup() {
	await setupRustfs();

	return async () => {
		await teardownRustfs();
	};
}