import { defineConfig } from "vitest/config";

import { project as exampleProject } from "./apps/example/vitest.config";
import { projects } from "./packages/s3qlite/vitest.config";

export default defineConfig({
	test: {
		projects: [
			...projects.map((project) => ({
				root: "./packages/s3qlite",
				...project,
			})),
			{ root: "./apps/example", ...exampleProject },
		],
	},
});