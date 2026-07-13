import { execFile } from "node:child_process";
import { rm } from "node:fs/promises";
import { promisify } from "node:util";

import { accessKeyId, bucket, endpoint, localDirectory, secretAccessKey } from "./config";

const run = promisify(execFile);

export default async function setup(): Promise<void> {
	await rm(localDirectory, { force: true, recursive: true });

	try {
		await run("mc", ["rm", "--recursive", "--force", `example/${bucket}`], {
			env: {
				...process.env,
				MC_HOST_example: `${endpoint.replace(/\/$/, "")}`.replace(
					"https://",
					`https://${accessKeyId}:${secretAccessKey}@`,
				),
			},
		});
	} catch (error) {
		throw new Error(
			`Could not empty bucket ${bucket}. Create it and grant the configured credentials access first.`,
			{ cause: error },
		);
	}
}