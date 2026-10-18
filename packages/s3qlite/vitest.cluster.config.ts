import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

// Multi-process cluster scenarios (local Postgres + RustFS containers). Slow; run with
// `bun run test:cluster`. The default config excludes them.
export default defineConfig({
	test: {
		name: "cluster",
		environment: "node",
		globalSetup: [fileURLToPath(new URL("./tests/cluster/setup.ts", import.meta.url))],
		include: ["tests/cluster/**/*.test.ts"],
		hookTimeout: 120_000,
		testTimeout: 240_000,
		// Scenarios kill and stop processes; run them one after another.
		fileParallelism: false,
		sequence: { concurrent: false },
	},
});