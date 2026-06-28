import { spawnSync } from "node:child_process";
import { readFile, readdir } from "node:fs/promises";
import { stdin as input, stdout as output } from "node:process";
import { createInterface } from "node:readline/promises";

const [command] = process.argv.slice(2);

const rl = createInterface({ input, output });

const run = ([bin, ...args]: [string, ...string[]]): void => {
	const result = spawnSync(bin, args, {
		cwd: process.cwd(),
		stdio: "inherit",
		env: process.env,
	});

	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
};

const getOutput = ([bin, ...args]: [string, ...string[]]): string => {
	const result = spawnSync(bin, args, {
		cwd: process.cwd(),
		stdio: ["ignore", "pipe", "inherit"],
		encoding: "utf8",
		env: process.env,
	});

	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}

	return result.stdout.trim();
};

const waitForCleanGit = async (message: string): Promise<void> => {
	while (true) {
		const status = getOutput(["git", "status", "--porcelain"]);

		if (status.length === 0) {
			return;
		}

		console.log(message);
		console.log(status);
		await rl.question("Press Enter after you fixed this.\n> ");
	}
};

const waitForCommit = async (message: string, previousHead: string): Promise<void> => {
	while (true) {
		console.log(message);
		await rl.question("Press Enter after you committed the changes.\n> ");

		const status = getOutput(["git", "status", "--porcelain"]);
		const nextHead = getOutput(["git", "rev-parse", "HEAD"]);

		if (status.length > 0) {
			console.log("Git tree is still dirty. Commit did not happen yet.");
			console.log(status);
			continue;
		}

		if (nextHead === previousHead) {
			console.log("HEAD did not change. You did not create a new commit yet.");
			continue;
		}

		return;
	}
};

const hasPendingChangeset = async (): Promise<boolean> => {
	const entries = await readdir(new URL("../.changeset/", import.meta.url), {
		withFileTypes: true,
	});

	return entries.some(
		(entry) => entry.isFile() && entry.name.endsWith(".md") && entry.name !== "README.md",
	);
};

const getPackageInfo = async (): Promise<{ name: string; version: string }> => {
	const packageJson = JSON.parse(
		await readFile(new URL("../packages/s3qlite/package.json", import.meta.url), "utf8"),
	) as { name: string; version: string };

	return {
		name: packageJson.name,
		version: packageJson.version,
	};
};

const isCurrentVersionPublished = async (): Promise<boolean> => {
	const { name, version } = await getPackageInfo();
	const result = spawnSync("npm", ["view", `${name}@${version}`, "version"], {
		cwd: process.cwd(),
		stdio: ["ignore", "pipe", "ignore"],
		encoding: "utf8",
		env: process.env,
	});

	return result.status === 0;
};

const ensureNpmAuth = async (): Promise<void> => {
	while (true) {
		const result = spawnSync("npm", ["whoami"], {
			cwd: process.cwd(),
			stdio: ["ignore", "pipe", "inherit"],
			encoding: "utf8",
			env: process.env,
		});

		if (result.status === 0) {
			console.log(`npm auth ok: ${result.stdout.trim()}`);
			return;
		}

		console.log("npm auth missing. Run `npm login`, then come back.");
		await rl.question("Press Enter after login.\n> ");
	}
};

const guide = async (): Promise<void> => {
	const packageInfo = await getPackageInfo();
	console.log(`Release guide for ${packageInfo.name}`);
	console.log(`Branch: ${getOutput(["git", "branch", "--show-current"])}`);
	console.log(`Package version: ${packageInfo.version}`);

	const currentVersionPublished = await isCurrentVersionPublished();
	const pendingChangeset = await hasPendingChangeset();
	const currentStatus = getOutput(["git", "status", "--porcelain"]);

	if (!pendingChangeset && currentStatus.length === 0 && !currentVersionPublished) {
		console.log("Release is already versioned and committed. Resuming publish only.");
		await ensureNpmAuth();

		const publishAnswer = (
			await rl.question("Publish to npm now? Type `publish` to continue.\n> ")
		).trim();
		if (publishAnswer !== "publish") {
			console.log("Publish cancelled.");
			return;
		}

		run(["bun", "run", "build"]);
		run(["bunx", "changeset", "publish"]);

		const pushAnswer = (
			await rl.question("Push commit and tags now? Type `push` to continue.\n> ")
		).trim();
		if (pushAnswer === "push") {
			run(["git", "push", "--follow-tags"]);
		}

		console.log("Release flow finished.");
		return;
	}

	if (!pendingChangeset) {
		console.log("No pending changeset found. Starting interactive changeset prompt.");
		run(["bunx", "changeset"]);

		if (!(await hasPendingChangeset())) {
			console.log("No changeset file was created. Aborting.");
			process.exit(1);
		}
	} else {
		console.log("Pending changeset found. Reusing it.");
	}

	run(["bunx", "changeset", "version"]);
	run(["bun", "run", "test:ci"]);
	run(["bun", "run", "build"]);

	const releaseHead = getOutput(["git", "rev-parse", "HEAD"]);
	await waitForCommit(
		"Commit the full release now. This single commit should include your code, changelog, and version bump.",
		releaseHead,
	);

	await waitForCleanGit("Git tree must stay clean before publishing.");
	await ensureNpmAuth();

	const publishAnswer = (
		await rl.question("Publish to npm now? Type `publish` to continue.\n> ")
	).trim();
	if (publishAnswer !== "publish") {
		console.log("Publish cancelled.");
		return;
	}

	run(["bun", "run", "build"]);
	run(["bunx", "changeset", "publish"]);

	const pushAnswer = (
		await rl.question("Push commit and tags now? Type `push` to continue.\n> ")
	).trim();
	if (pushAnswer === "push") {
		run(["git", "push", "--follow-tags"]);
	}

	console.log("Release flow finished.");
};

const ensureCleanGit = (): void => {
	const status = getOutput(["git", "status", "--porcelain"]);

	if (status.length > 0) {
		console.error("Git working tree is not clean. Commit or stash changes first.");
		process.exit(1);
	}
};

const main = async (): Promise<void> => {
	try {
		switch (command) {
			case "guide": {
				await guide();
				break;
			}

			case "version": {
				ensureCleanGit();
				run(["bunx", "changeset", "version"]);
				console.log(
					"Release files updated. Commit them, then run `bun run release:publish`.",
				);
				break;
			}

			case "publish": {
				ensureCleanGit();
				await ensureNpmAuth();
				run(["bun", "run", "build"]);
				run(["bunx", "changeset", "publish"]);
				console.log("Published. Push commit and tags with `git push --follow-tags`.");
				break;
			}

			default: {
				console.error("Usage: bun scripts/release.ts <guide|version|publish>");
				process.exit(1);
			}
		}
	} finally {
		rl.close();
	}
};

await main();