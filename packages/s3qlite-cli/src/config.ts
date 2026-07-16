import { Data, Effect, Option } from "effect";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";

export type Alias = {
	readonly accessKey: string;
	readonly endpoint: string;
	readonly path: "auto" | "off" | "on";
	readonly region: string;
	readonly secretKey: string;
};

type Config = {
	readonly aliases: Readonly<Record<string, Alias>>;
	readonly version: 1;
};

export class CliError extends Data.TaggedError("CliError")<{
	readonly message: string;
}> {}

const emptyConfig: Config = { aliases: {}, version: 1 };

const isAlias = (value: unknown): value is Alias => {
	if (typeof value !== "object" || Option.isNone(Option.fromNullable(value))) {
		return false;
	}

	const alias = value as Record<string, unknown>;
	return (
		typeof alias.accessKey === "string" &&
		typeof alias.endpoint === "string" &&
		(alias.path === "auto" || alias.path === "off" || alias.path === "on") &&
		typeof alias.region === "string" &&
		typeof alias.secretKey === "string"
	);
};

const parseConfig = (text: string, path: string): Effect.Effect<Config, CliError> =>
	Effect.try({
		try: () => JSON.parse(text) as unknown,
		catch: () => new CliError({ message: `invalid config file "${path}"` }),
	}).pipe(
		Effect.flatMap((value) => {
			if (typeof value !== "object" || Option.isNone(Option.fromNullable(value))) {
				return Effect.fail(new CliError({ message: `invalid config file "${path}"` }));
			}

			const config = value as { aliases?: unknown; version?: unknown };
			const aliases = Option.fromNullable(config.aliases);
			if (
				config.version !== 1 ||
				typeof config.aliases !== "object" ||
				Option.isNone(aliases) ||
				!Object.values(aliases.value).every(isAlias)
			) {
				return Effect.fail(new CliError({ message: `invalid config file "${path}"` }));
			}

			return Effect.succeed(config as Config);
		}),
	);

export const readConfig = (directory: string): Effect.Effect<Config, CliError> => {
	const path = join(directory, "config.json");
	return Effect.tryPromise({
		try: () => readFile(path, "utf8"),
		catch: (error) => ({ error }),
	}).pipe(
		Effect.catchAll(({ error }) => {
			const cause = Option.fromNullable(error);
			return typeof error === "object" &&
				Option.isSome(cause) &&
				"code" in cause.value &&
				cause.value.code === "ENOENT"
				? Effect.succeed(emptyConfig)
				: Effect.fail(new CliError({ message: `failed to read config file "${path}"` }));
		}),
		Effect.flatMap((text) =>
			typeof text === "string" ? parseConfig(text, path) : Effect.succeed(text),
		),
	);
};

export const writeConfig = (directory: string, config: Config): Effect.Effect<void, CliError> =>
	Effect.tryPromise({
		try: async () => {
			await mkdir(directory, { mode: 0o700, recursive: true });
			await chmod(directory, 0o700);
			const path = join(directory, "config.json");
			const temporaryPath = `${path}.${crypto.randomUUID()}.tmp`;
			await writeFile(
				temporaryPath,
				`${JSON.stringify(config, (_key, value) => value, 2)}\n`,
				{
					mode: 0o600,
				},
			);
			await rename(temporaryPath, path);
			await chmod(path, 0o600);
		},
		catch: () => new CliError({ message: `failed to write config in "${directory}"` }),
	});

export const getAlias = (directory: string, name: string): Effect.Effect<Alias, CliError> =>
	readConfig(directory).pipe(
		Effect.flatMap((config) => {
			if (!Object.hasOwn(config.aliases, name)) {
				return Effect.fail(new CliError({ message: `alias "${name}" not found` }));
			}

			return Effect.succeed(config.aliases[name]);
		}),
	);

export const setAlias = (
	directory: string,
	name: string,
	alias: Alias,
): Effect.Effect<boolean, CliError> =>
	readConfig(directory).pipe(
		Effect.flatMap((config) =>
			writeConfig(directory, {
				aliases: { ...config.aliases, [name]: alias },
				version: 1,
			}).pipe(Effect.as(Object.hasOwn(config.aliases, name))),
		),
	);

export const removeAlias = (directory: string, name: string): Effect.Effect<void, CliError> =>
	readConfig(directory).pipe(
		Effect.flatMap((config) => {
			if (!Object.hasOwn(config.aliases, name)) {
				return Effect.fail(new CliError({ message: `alias "${name}" not found` }));
			}

			const aliases = { ...config.aliases };
			delete aliases[name];
			return writeConfig(directory, { aliases, version: 1 });
		}),
	);