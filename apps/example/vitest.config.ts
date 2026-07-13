import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const globalSetup = fileURLToPath(new URL("./setup.ts", import.meta.url));

export const project = {
	test: {
		name: "example",
		environment: "node",
		globalSetup: [globalSetup],
		hookTimeout: 60000,
		testTimeout: 60000,
		include: ["promise.ts", "effect.ts", "fork.ts", "transaction-mode.ts"],
	},
};

export default defineConfig({ test: project.test });