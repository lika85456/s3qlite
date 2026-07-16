import { S3 } from "@effect-aws/client-s3";
import { NodeContext } from "@effect/platform-node";
import { Effect } from "effect";
import { chmod, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { Readable } from "node:stream";
import { describe, expect, it, vi } from "vitest";

import { Rustfs, RustfsLive } from "../../s3qlite/tests/utils/rustfs.js";
import { runCli } from "../src/cli.js";
import { getAlias } from "../src/config.js";

type Result = {
	readonly code: number;
	readonly stderr: string;
	readonly stdout: string;
};

const run = async (args: readonly string[], stdin = ""): Promise<Result> => {
	const stdout: string[] = [];
	const stderr: string[] = [];
	const log = vi.spyOn(console, "log").mockImplementation((value) => stdout.push(String(value)));
	const error = vi
		.spyOn(console, "error")
		.mockImplementation((value) => stderr.push(String(value)));

	try {
		await Effect.runPromise(
			runCli(["node", "s3qlite", ...args], Readable.from([stdin])).pipe(
				Effect.provide(NodeContext.layer),
			),
		);
		return { code: 0, stderr: stderr.join("\n"), stdout: stdout.join("\n") };
	} catch {
		return { code: 1, stderr: stderr.join("\n"), stdout: stdout.join("\n") };
	} finally {
		log.mockRestore();
		error.mockRestore();
	}
};

const fixture = Effect.acquireRelease(
	Effect.gen(function* () {
		const rustfs = yield* Rustfs;
		const s3 = yield* S3;
		const root = yield* Effect.tryPromise(() => mkdtemp("/tmp/opencode/s3qlite-cli-"));
		const bucket = rustfs.makeBucketName("cli");
		yield* s3.createBucket({ Bucket: bucket });

		const configDirectory = join(root, "config");
		const dataDirectory = join(root, "data");
		const globalArgs = ["--config-dir", configDirectory, "--data-dir", dataDirectory];
		const aliasResult = yield* Effect.promise(() =>
			run([
				...globalArgs,
				"alias",
				"set",
				"local",
				rustfs.endpoint,
				rustfs.accessKey,
				rustfs.secretKey,
				"--region",
				rustfs.region,
			]),
		);
		expect(aliasResult, JSON.stringify(aliasResult)).toMatchObject({ code: 0, stderr: "" });

		return { bucket, configDirectory, dataDirectory, globalArgs, root, rustfs };
	}),
	({ root }) => Effect.promise(() => rm(root, { force: true, recursive: true })),
);

// oxlint-disable-next-line local-rules/function-minimum-length -- Keeps every integration test scoped to temporary storage.
const withFixture = <A, E, R>(
	test: (value: Effect.Effect.Success<typeof fixture>) => Effect.Effect<A, E, R>,
) => {
	const scoped = Effect.scoped(Effect.flatMap(fixture, test));
	return scoped.pipe(Effect.provide(RustfsLive));
};

describe("s3qlite CLI", () => {
	it("stores, lists, overwrites, and removes mc-style aliases", () =>
		Effect.runPromise(
			withFixture(({ configDirectory, globalArgs, rustfs }) =>
				Effect.gen(function* () {
					const configPath = join(configDirectory, "config.json");
					const config = yield* Effect.promise(() => readFile(configPath, "utf8"));
					expect(config).toContain(rustfs.accessKey);
					expect(config).toContain(rustfs.secretKey);
					expect((yield* Effect.promise(() => stat(configPath))).mode & 0o777).toBe(
						0o600,
					);

					const list = yield* Effect.promise(() => run([...globalArgs, "alias", "list"]));
					expect(list).toEqual({
						code: 0,
						stderr: "",
						stdout: `local\t${rustfs.endpoint}\tpath=auto`,
					});
					expect(list.stdout).not.toContain(rustfs.accessKey);
					expect(list.stdout).not.toContain(rustfs.secretKey);

					const overwrite = yield* Effect.promise(() =>
						run([
							...globalArgs,
							"alias",
							"set",
							"local",
							rustfs.endpoint,
							"replacement-key",
							"replacement-secret",
						]),
					);
					expect(overwrite.stdout).toBe('Updated alias "local".');

					const removed = yield* Effect.promise(() =>
						run([...globalArgs, "alias", "remove", "local"]),
					);
					expect(removed.stdout).toBe('Removed alias "local".');
					expect(
						(yield* Effect.promise(() => run([...globalArgs, "alias", "list"]))).stdout,
					).toBe("");
					const reservedName = yield* Effect.either(
						getAlias(configDirectory, "constructor"),
					);
					expect(reservedName).toMatchObject({
						_tag: "Left",
						left: { message: 'alias "constructor" not found' },
					});
				}),
			),
		));

	it("creates databases, lists their heads, and rejects duplicates", () =>
		Effect.runPromise(
			withFixture(({ bucket, globalArgs }) =>
				Effect.gen(function* () {
					const target = `local/${bucket}/customers`;
					const created = yield* Effect.promise(() =>
						run([...globalArgs, "create", target]),
					);
					expect(created).toEqual({
						code: 0,
						stderr: "",
						stdout: `Created "${target}".`,
					});
					yield* Effect.promise(() =>
						run([...globalArgs, "create", `local/${bucket}/orders`]),
					);
					const listed = yield* Effect.promise(() =>
						run([...globalArgs, "ls", `local/${bucket}`]),
					);
					expect(listed).toEqual({
						code: 0,
						stderr: "",
						stdout: "customers\norders",
					});

					const duplicate = yield* Effect.promise(() =>
						run([...globalArgs, "create", target]),
					);
					expect(duplicate.code).toBe(1);
					expect(duplicate.stderr).toContain('database "customers" already exists');
				}),
			),
		));

	it("executes reader and writer SQL from arguments and stdin", () =>
		Effect.runPromise(
			withFixture(({ bucket, globalArgs }) =>
				Effect.gen(function* () {
					const target = `local/${bucket}/sql-db`;
					yield* Effect.promise(() => run([...globalArgs, "create", target]));

					const ddl = yield* Effect.promise(() =>
						run([
							...globalArgs,
							"sql",
							target,
							"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
						]),
					);
					expect(JSON.parse(ddl.stdout)).toEqual({
						changes: 0,
						lastInsertRowid: expect.any(Number),
					});

					const insert = yield* Effect.promise(() =>
						run([
							...globalArgs,
							"sql",
							target,
							"INSERT INTO users (name) VALUES ('Alice')",
						]),
					);
					const insertResult = JSON.parse(insert.stdout) as {
						changes: number;
						lastInsertRowid: number;
					};
					expect(insertResult).toEqual({
						changes: expect.any(Number),
						lastInsertRowid: expect.any(Number),
					});
					expect(insertResult.changes).toBeGreaterThan(0);

					const select = yield* Effect.promise(() =>
						run(
							[...globalArgs, "sql", target],
							"SELECT id, name FROM users ORDER BY id",
						),
					);
					expect(JSON.parse(select.stdout)).toEqual([{ id: 1, name: "Alice" }]);
				}),
			),
		));

	it("forks across buckets and keeps the fork after removing its source", () =>
		Effect.runPromise(
			withFixture(({ bucket, globalArgs, rustfs }) =>
				Effect.gen(function* () {
					const source = `local/${bucket}/source`;
					const destinationBucket = rustfs.makeBucketName("fork-destination");
					const target = `local/${destinationBucket}/target`;
					const s3 = yield* S3;
					yield* s3.createBucket({ Bucket: destinationBucket });
					yield* Effect.promise(() => run([...globalArgs, "create", source]));
					yield* Effect.promise(() =>
						run([...globalArgs, "sql", source, "CREATE TABLE values_ (value TEXT)"]),
					);
					yield* Effect.promise(() =>
						run([
							...globalArgs,
							"sql",
							source,
							"INSERT INTO values_ VALUES ('source')",
						]),
					);

					const forked = yield* Effect.promise(() =>
						run([...globalArgs, "fork", source, target]),
					);
					expect(forked.stdout).toBe(`Forked "${source}" to "${target}".`);

					const needsForce = yield* Effect.promise(() =>
						run([...globalArgs, "remove", source]),
					);
					expect(needsForce.code).toBe(1);
					expect(needsForce.stderr).toContain("--force");

					const removed = yield* Effect.promise(() =>
						run([...globalArgs, "remove", source, "--force"]),
					);
					expect(removed.stdout).toBe(`Removed "${source}".`);
					const recreated = yield* Effect.promise(() =>
						run([...globalArgs, "create", source]),
					);
					expect(recreated).toEqual({
						code: 0,
						stderr: "",
						stdout: `Created "${source}".`,
					});

					const rows = yield* Effect.promise(() =>
						run([...globalArgs, "sql", target, "SELECT value FROM values_"]),
					);
					expect(JSON.parse(rows.stdout)).toEqual([{ value: "source" }]);
				}),
			),
		));

	it("rejects unknown aliases, malformed targets, and missing databases", () =>
		Effect.runPromise(
			withFixture(({ bucket, globalArgs }) =>
				Effect.gen(function* () {
					const unknown = yield* Effect.promise(() =>
						run([...globalArgs, "create", `missing/${bucket}/db`]),
					);
					expect(unknown.code).toBe(1);
					expect(unknown.stderr).toContain('alias "missing" not found');

					const malformed = yield* Effect.promise(() =>
						run([...globalArgs, "create", "local/only-bucket"]),
					);
					expect(malformed.code).toBe(1);
					expect(malformed.stderr).toContain("ALIAS/BUCKET/DATABASE");

					const missing = yield* Effect.promise(() =>
						run([...globalArgs, "sql", `local/${bucket}/missing`, "SELECT 1"]),
					);
					expect(missing.code).toBe(1);
					expect(missing.stderr).toContain('database "missing" does not exist');
				}),
			),
		));

	it("provides command help without reading the default home directory", async () => {
		const root = await mkdtemp("/tmp/opencode/s3qlite-cli-help-");
		await chmod(root, 0o700);
		try {
			const result = await run([
				"--help",
				"--config-dir",
				join(root, "config"),
				"--data-dir",
				join(root, "data"),
			]);
			expect(result.code, JSON.stringify(result)).toBe(0);
			expect(result.stdout).not.toContain("\n\n");
			expect(result.stdout).toContain("Directory containing CLI alias configuration");
			expect(result.stdout).toContain("create");
			expect(result.stdout).toContain("remove");
			expect(result.stdout).toContain("fork");
			expect(result.stdout).toContain("sql");
		} finally {
			await rm(root, { force: true, recursive: true });
		}
	});
});