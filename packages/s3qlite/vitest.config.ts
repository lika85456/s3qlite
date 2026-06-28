import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const globalSetup = fileURLToPath(new URL("./tests/utils/rustfs.ts", import.meta.url));

export default defineConfig({
	test: {
		projects: [
			{
				test: {
					name: "integration",
					environment: "node",
					globalSetup: [globalSetup],
					hookTimeout: 60000,
					testTimeout: 60000,
					include: ["tests/**/*.test.ts", "src/**/*.test.ts"],
					exclude: ["**/fuzzy*"],
				},
			},
			{
				test: {
					name: "fuzzy",
					environment: "node",
					hookTimeout: 60000,
					testTimeout: 60000,
					include: ["**/fuzzy*.test.ts"],
				},
			},
		],
	},
});