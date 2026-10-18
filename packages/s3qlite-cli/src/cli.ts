#!/usr/bin/env node

import { S3 } from "@effect-aws/client-s3";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { connect, makeS3KV } from "@lika85456/s3qlite";
import type { Head } from "@lika85456/s3qlite";
import { Console, Effect, Option, Redacted, Stream } from "effect";
import { Argument, Command, Flag, Prompt } from "effect/cli";
import { realpathSync } from "node:fs";
import { rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";

import { CliError, getAlias, readConfig, removeAlias, setAlias } from "./config.js";
import type { Alias } from "./config.js";

type Target = {
	readonly alias: string;
	readonly bucket: string;
	readonly database: string;
	readonly value: string;
};

type BucketTarget = {
	readonly alias: string;
	readonly bucket: string;
	readonly value: string;
};

const defaultDirectory = join(homedir(), ".s3qlite");
const configDirectory = Flag.String("config-dir").pipe(
	Flag.withDescription("Directory containing CLI alias configuration"),
	Flag.withDefault(defaultDirectory),
);
const dataDirectory = Flag.String("data-dir").pipe(
	Flag.withDescription("Directory for local database files"),
	Flag.withDefault(join(defaultDirectory, "data")),
);

const root = Command.make("s3qlite").pipe(
	Command.withSharedFlags({ configDirectory, dataDirectory }),
);

const parseTarget = (value: string): Effect.Effect<Target, CliError> => {
	const parts = value.split("/");
	if (
		parts.length !== 3 ||
		parts.some((part) => part === "" || part === "." || part === ".." || part.includes("\\"))
	) {
		return Effect.fail(
			new CliError({ message: `target must use ALIAS/BUCKET/DATABASE: "${value}"` }),
		);
	}

	const [alias, bucket, database] = parts;
	return Effect.succeed({ alias, bucket, database, value });
};

const parseBucketTarget = (value: string): Effect.Effect<BucketTarget, CliError> => {
	const parts = value.split("/");
	if (
		parts.length !== 2 ||
		parts.some((part) => part === "" || part === "." || part === ".." || part.includes("\\"))
	) {
		return Effect.fail(new CliError({ message: `target must use ALIAS/BUCKET: "${value}"` }));
	}

	const [alias, bucket] = parts;
	return Effect.succeed({ alias, bucket, value });
};

const s3Config = (alias: Alias, forcePathStyle: boolean) => ({
	credentials: {
		accessKeyId: alias.accessKey,
		secretAccessKey: alias.secretKey,
	},
	endpoint: alias.endpoint,
	forcePathStyle,
	region: alias.region,
});

const resolveS3Config = (bucket: string, alias: Alias) => {
	if (alias.path !== "auto") {
		return Effect.succeed(s3Config(alias, alias.path === "on"));
	}

	const probe = (forcePathStyle: boolean) =>
		S3.pipe(
			Effect.flatMap((s3) => s3.headBucket({ Bucket: bucket })),
			Effect.as(s3Config(alias, forcePathStyle)),
			Effect.provide(S3.layer(s3Config(alias, forcePathStyle))),
		);

	return probe(false).pipe(
		Effect.catch(() => probe(true)),
		Effect.mapError(() => new CliError({ message: `failed to access bucket "${bucket}"` })),
	);
};

const isMissingObject = (error: unknown): boolean => {
	if (typeof error !== "object" || Option.isNone(Option.fromNullishOr(error))) {
		return false;
	}

	const value = error as { Code?: unknown; name?: unknown; $metadata?: unknown };
	if (value.Code === "NoSuchKey" || value.name === "NoSuchKey" || value.name === "NotFound") {
		return true;
	}

	const metadata = Option.fromNullishOr(value.$metadata);
	return Option.exists(
		metadata,
		(current) =>
			typeof current === "object" &&
			"httpStatusCode" in current &&
			current.httpStatusCode === 404,
	);
};

const databaseExists = (
	bucket: string,
	database: string,
	clientConfig: ReturnType<typeof s3Config>,
) =>
	Effect.gen(function* () {
		const s3 = yield* S3;
		const result = yield* Effect.result(
			s3.headObject({ Bucket: bucket, Key: `${database}.json` }),
		);
		if (result._tag === "Success") {
			return true;
		}
		if (isMissingObject(result.failure)) {
			return false;
		}
		return yield* Effect.fail(new CliError({ message: `failed to access bucket "${bucket}"` }));
	}).pipe(Effect.provide(S3.layer(clientConfig)));

const resolveTarget = (value: string, directory: string) =>
	parseTarget(value).pipe(
		Effect.flatMap((target) =>
			getAlias(directory, target.alias).pipe(Effect.map((alias) => ({ alias, target }))),
		),
	);

const resolveBucketTarget = (value: string, directory: string) =>
	parseBucketTarget(value).pipe(
		Effect.flatMap((target) =>
			getAlias(directory, target.alias).pipe(Effect.map((alias) => ({ alias, target }))),
		),
	);

const openDatabase = (
	target: Target,
	clientConfig: ReturnType<typeof s3Config>,
	directory: string,
) =>
	Effect.acquireRelease(
		Effect.tryPromise({
			try: () =>
				connect(target.database, {
					bucket: target.bucket,
					localDirectory: join(directory, target.alias, target.bucket),
					s3: clientConfig,
				}),
			catch: (error) =>
				new CliError({
					message: error instanceof Error ? error.message : "failed to open database",
				}),
		}),
		(database) => Effect.tryPromise(() => database.close()).pipe(Effect.orDie),
	);

const requireDatabase = (target: Target, clientConfig: ReturnType<typeof s3Config>) =>
	databaseExists(target.bucket, target.database, clientConfig).pipe(
		Effect.flatMap((exists) =>
			exists
				? Effect.void
				: Effect.fail(
						new CliError({ message: `database "${target.database}" does not exist` }),
					),
		),
	);

const forkAcrossBuckets = (
	source: Target,
	destination: Target,
	sourceConfig: ReturnType<typeof s3Config>,
	destinationConfig: ReturnType<typeof s3Config>,
) =>
	Effect.gen(function* () {
		const sourceRemote = yield* makeS3KV(source.bucket).pipe(
			Effect.provide(S3.layer(sourceConfig)),
		);
		const destinationRemote = yield* makeS3KV(destination.bucket).pipe(
			Effect.provide(S3.layer(destinationConfig)),
		);
		const sourceHead = yield* sourceRemote.get(`${source.database}.json`);
		if (Option.isNone(sourceHead)) {
			return yield* Effect.fail(
				new CliError({ message: `database "${source.database}" does not exist` }),
			);
		}

		const head = yield* Effect.try({
			try: () => JSON.parse(new TextDecoder().decode(sourceHead.value.value)) as Head,
			catch: () => new CliError({ message: `failed to read database "${source.value}"` }),
		});
		for (const key of [
			...head.snapshots.map((snapshot) => `${snapshot.id}.snapshot`),
			...head.batches.map((batch) => `${batch.id}.batch`),
		]) {
			const object = yield* sourceRemote.readStream(key);
			if (Option.isNone(object)) {
				return yield* Effect.fail(
					new CliError({ message: `database "${source.value}" is incomplete` }),
				);
			}
			yield* destinationRemote.writeStream(key, object.value);
		}
		yield* destinationRemote.create(`${destination.database}.json`, sourceHead.value.value);
	}).pipe(
		Effect.mapError((error) =>
			error instanceof CliError
				? error
				: new CliError({
						message: `failed to fork "${source.value}" to "${destination.value}"`,
					}),
		),
	);

const aliasName = Argument.String("alias").pipe(Argument.withDescription("Alias name"));
const endpoint = Argument.String("url").pipe(
	Argument.withDescription("S3-compatible endpoint URL"),
);
const accessKey = Argument.String("access-key").pipe(
	Argument.withDescription("S3 access key"),
	Argument.optional,
);
const secretKey = Argument.Redacted("secret-key").pipe(
	Argument.withDescription("S3 secret key"),
	Argument.optional,
);
const region = Flag.String("region").pipe(
	Flag.withDescription("S3 region"),
	Flag.withDefault("us-east-1"),
);
const pathStyle = Flag.Literals("path", ["auto", "on", "off"] as const).pipe(
	Flag.withDescription("Use path-style S3 URLs"),
	Flag.withDefault("auto" as const),
);

const aliasSet = Command.make(
	"set",
	{ aliasName, endpoint, accessKey, secretKey, pathStyle, region },
	({ accessKey, aliasName, endpoint, pathStyle, region, secretKey }) =>
		Effect.gen(function* () {
			const { configDirectory } = yield* root;
			if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(aliasName)) {
				return yield* Effect.fail(
					new CliError({ message: `invalid alias "${aliasName}"` }),
				);
			}
			yield* Effect.try({
				try: () => new URL(endpoint),
				catch: () => new CliError({ message: `invalid endpoint "${endpoint}"` }),
			});

			const resolvedAccessKey = Option.isSome(accessKey)
				? accessKey.value
				: yield* Prompt.run(Prompt.String({ message: "Access Key" }));
			const resolvedSecretKey = Option.isSome(secretKey)
				? secretKey.value
				: yield* Prompt.run(Prompt.Hidden({ message: "Secret Key" }));
			const updated = yield* setAlias(configDirectory, aliasName, {
				accessKey: resolvedAccessKey,
				endpoint,
				path: pathStyle,
				region,
				secretKey: Redacted.value(resolvedSecretKey),
			});
			yield* Console.log(`${updated ? "Updated" : "Added"} alias "${aliasName}".`);
		}),
);

const aliasList = Command.make("list", {}, () =>
	Effect.gen(function* () {
		const { configDirectory } = yield* root;
		const config = yield* readConfig(configDirectory);
		for (const [name, alias] of Object.entries(config.aliases).sort(([a], [b]) =>
			a.localeCompare(b),
		)) {
			yield* Console.log(`${name}\t${alias.endpoint}\tpath=${alias.path}`);
		}
	}),
);

const aliasRemove = Command.make("remove", { aliasName }, ({ aliasName }) =>
	Effect.gen(function* () {
		const { configDirectory } = yield* root;
		yield* removeAlias(configDirectory, aliasName);
		yield* Console.log(`Removed alias "${aliasName}".`);
	}),
);

const aliasCommand = Command.make("alias").pipe(
	Command.withSubcommands([aliasSet, aliasList, aliasRemove]),
);

const targetArgument = Argument.String("ALIAS/BUCKET/DATABASE").pipe(
	Argument.withDescription("Database target"),
);

const createCommand = Command.make("create", { value: targetArgument }, ({ value }) =>
	Effect.gen(function* () {
		const globals = yield* root;
		const { alias, target } = yield* resolveTarget(value, globals.configDirectory);
		const clientConfig = yield* resolveS3Config(target.bucket, alias);
		if (yield* databaseExists(target.bucket, target.database, clientConfig)) {
			return yield* Effect.fail(
				new CliError({ message: `database "${target.database}" already exists` }),
			);
		}

		yield* Effect.scoped(openDatabase(target, clientConfig, globals.dataDirectory));
		yield* Console.log(`Created "${target.value}".`);
	}),
);

const bucketArgument = Argument.String("ALIAS/BUCKET").pipe(
	Argument.withDescription("Bucket containing database heads"),
);
const listCommand = Command.make("ls", { value: bucketArgument }, ({ value }) =>
	Effect.gen(function* () {
		const globals = yield* root;
		const { alias, target } = yield* resolveBucketTarget(value, globals.configDirectory);
		const clientConfig = yield* resolveS3Config(target.bucket, alias);
		const heads = yield* S3.pipe(
			Effect.flatMap((s3) =>
				s3.listObjectsV2Stream({ Bucket: target.bucket }).pipe(
					Stream.flatMap((page) => Stream.fromIterable(page.Contents ?? [])),
					Stream.runCollect,
				),
			),
			Effect.map((objects) =>
				objects
					.flatMap((object) => Option.toArray(Option.fromNullishOr(object.Key)))
					.filter((key) => key.endsWith(".json"))
					.sort(),
			),
			Effect.mapError(
				() => new CliError({ message: `failed to access bucket "${target.bucket}"` }),
			),
			Effect.provide(S3.layer(clientConfig)),
		);
		yield* Console.log(heads.map((key) => key.slice(0, -".json".length)).join("\n"));
	}),
);

const force = Flag.Boolean("force").pipe(
	Flag.withDescription("Confirm database removal"),
	Flag.withDefault(false),
);
const removeCommand = Command.make("remove", { force, value: targetArgument }, ({ force, value }) =>
	Effect.gen(function* () {
		if (!force) {
			return yield* Effect.fail(
				new CliError({ message: "database removal requires --force" }),
			);
		}

		const globals = yield* root;
		const { alias, target } = yield* resolveTarget(value, globals.configDirectory);
		const clientConfig = yield* resolveS3Config(target.bucket, alias);
		yield* requireDatabase(target, clientConfig);
		yield* S3.pipe(
			Effect.flatMap((s3) =>
				s3.deleteObject({ Bucket: target.bucket, Key: `${target.database}.json` }),
			),
			Effect.asVoid,
			Effect.mapError(() => new CliError({ message: `failed to remove "${target.value}"` })),
			Effect.provide(S3.layer(clientConfig)),
		);
		const directory = join(globals.dataDirectory, target.alias, target.bucket);
		yield* Effect.tryPromise({
			try: () =>
				Promise.all(
					["json", "db", "db-wal", "db-shm"].map((suffix) =>
						rm(join(directory, `${target.database}.${suffix}`), { force: true }),
					),
				),
			catch: () =>
				new CliError({ message: `failed to clear local database "${target.value}"` }),
		});
		yield* Console.log(`Removed "${target.value}".`);
	}),
);

const sourceArgument = Argument.String("source").pipe(
	Argument.withDescription("Existing database target"),
);
const destinationArgument = Argument.String("target").pipe(
	Argument.withDescription("New database target"),
);
const forkCommand = Command.make(
	"fork",
	{ sourceValue: sourceArgument, destinationValue: destinationArgument },
	({ destinationValue, sourceValue }) =>
		Effect.gen(function* () {
			const globals = yield* root;
			const source = yield* resolveTarget(sourceValue, globals.configDirectory);
			const destination = yield* resolveTarget(destinationValue, globals.configDirectory);
			if (source.target.value === destination.target.value) {
				return yield* Effect.fail(
					new CliError({
						message: "fork source and target must differ",
					}),
				);
			}

			const sourceConfig = yield* resolveS3Config(source.target.bucket, source.alias);
			const destinationConfig = yield* resolveS3Config(
				destination.target.bucket,
				destination.alias,
			);
			yield* requireDatabase(source.target, sourceConfig);
			if (
				yield* databaseExists(
					destination.target.bucket,
					destination.target.database,
					destinationConfig,
				)
			) {
				return yield* Effect.fail(
					new CliError({
						message: `database "${destination.target.database}" already exists`,
					}),
				);
			}

			yield* forkAcrossBuckets(
				source.target,
				destination.target,
				sourceConfig,
				destinationConfig,
			);
			yield* Console.log(`Forked "${source.target.value}" to "${destination.target.value}".`);
		}),
);

const makeSyncCommand = (operation: "pull" | "push" | "sync") =>
	Command.make(operation, { value: targetArgument }, ({ value }) =>
		Effect.gen(function* () {
			const globals = yield* root;
			const { alias, target } = yield* resolveTarget(value, globals.configDirectory);
			const clientConfig = yield* resolveS3Config(target.bucket, alias);
			yield* requireDatabase(target, clientConfig);
			yield* Effect.scoped(
				openDatabase(target, clientConfig, globals.dataDirectory).pipe(
					Effect.flatMap((database) =>
						Effect.tryPromise(() => database[operation]()).pipe(
							Effect.mapError(
								(error) =>
									new CliError({
										message:
											error instanceof Error
												? error.message
												: `failed to ${operation} database`,
									}),
							),
						),
					),
				),
			);
			yield* Console.log(
				`${operation[0].toUpperCase()}${operation.slice(1)}ed "${target.value}".`,
			);
		}),
	);

const pullCommand = makeSyncCommand("pull");
const pushCommand = makeSyncCommand("push");
const syncCommand = makeSyncCommand("sync");

const sqlArgument = Argument.String("sql").pipe(
	Argument.withDescription("SQL statement, or read from stdin when omitted"),
	Argument.optional,
);

const readInput = (input: Readable): Effect.Effect<string, CliError> =>
	Effect.tryPromise({
		try: async () => {
			let text = "";
			for await (const chunk of input) {
				text += String(chunk);
			}
			return text;
		},
		catch: () => new CliError({ message: "failed to read SQL from stdin" }),
	});

const stringify = (value: unknown): string =>
	JSON.stringify(value, (_key, item: unknown) => {
		if (typeof item === "bigint") {
			return item.toString();
		}
		if (item instanceof Uint8Array) {
			return { base64: Buffer.from(item).toString("base64") };
		}
		return item;
	});

const makeSqlCommand = (input: Readable) =>
	Command.make("sql", { value: targetArgument, sql: sqlArgument }, ({ value, sql }) =>
		Effect.gen(function* () {
			const globals = yield* root;
			const { alias, target } = yield* resolveTarget(value, globals.configDirectory);
			const clientConfig = yield* resolveS3Config(target.bucket, alias);
			yield* requireDatabase(target, clientConfig);
			const source = Option.isSome(sql) ? sql.value : (yield* readInput(input)).trim();
			if (source === "") {
				return yield* Effect.fail(new CliError({ message: "SQL must not be empty" }));
			}

			const result = yield* Effect.scoped(
				openDatabase(target, clientConfig, globals.dataDirectory).pipe(
					Effect.flatMap((database) =>
						Effect.tryPromise(async () => {
							await database.sync();
							const statement = await database.prepare(source);
							try {
								const result = statement.reader
									? await statement.all()
									: await statement.run();
								if (!statement.reader) {
									await database.sync();
								}
								return result;
							} finally {
								statement.close();
							}
						}).pipe(
							Effect.mapError(
								(error) =>
									new CliError({
										message:
											error instanceof Error
												? error.message
												: "SQL execution failed",
									}),
							),
						),
					),
				),
			);
			yield* Console.log(stringify(result));
		}),
	);

const makeCommand = (input: Readable) =>
	root.pipe(
		Command.withSubcommands([
			aliasCommand,
			createCommand,
			listCommand,
			removeCommand,
			forkCommand,
			pullCommand,
			pushCommand,
			syncCommand,
			makeSqlCommand(input),
		]),
	);

export const runCli = (args: readonly string[], input: Readable = process.stdin) =>
	Console.consoleWith((console) =>
		Command.runWith(makeCommand(input), { version: "0.1.0" })(args.slice(2)).pipe(
			Effect.provideService(Console.Console, {
				...console,
				log: (...values) =>
					console.log(
						...values.map((value) =>
							typeof value === "string" ? value.replaceAll(/\n{2,}/g, "\n") : value,
						),
					),
			}),
		),
	).pipe(
		Effect.catch((error) => {
			const message = error instanceof CliError ? error.message : String(error);
			return Console.error(`error: ${message}`).pipe(Effect.andThen(Effect.fail(error)));
		}),
	);

if (process.argv[1] && fileURLToPath(import.meta.url) === realpathSync(process.argv[1])) {
	runCli(process.argv).pipe(
		Effect.provide(NodeServices.layer),
		NodeRuntime.runMain({ disableErrorReporting: true }),
	);
}