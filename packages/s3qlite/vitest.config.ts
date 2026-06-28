import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const globalSetup = fileURLToPath(new URL("./tests/utils/rustfs.ts", import.meta.url));

export default defineConfig({
	test: {
		environment: "node",
		globalSetup: [globalSetup],
		hookTimeout: 60000,
		testTimeout: 60000,
	},
});