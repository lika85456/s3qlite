import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
	resolve: {
		alias: {
			"@lika85456/s3qlite": fileURLToPath(new URL("../s3qlite/index.ts", import.meta.url)),
		},
	},
	test: {
		environment: "node",
		globalSetup: [fileURLToPath(new URL("../s3qlite/tests/utils/rustfs.ts", import.meta.url))],
		hookTimeout: 60_000,
		testTimeout: 60_000,
	},
});