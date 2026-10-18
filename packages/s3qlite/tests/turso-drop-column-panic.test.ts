import { describe, expect, it } from "@effect/vitest";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";

const fixture = fileURLToPath(new URL("./fixtures/turso-drop-column.ts", import.meta.url));

const runFixture = (args: readonly string[]) =>
	new Promise<{ readonly code: number; readonly stdout: string; readonly stderr: string }>(
		(resolve) => {
			execFile("bun", [fixture, ...args], { timeout: 60_000 }, (error, stdout, stderr) => {
				resolve({
					code: error ? (typeof error.code === "number" ? error.code : 1) : 0,
					stdout,
					stderr,
				});
			});
		},
	);

// Regression: Turso aborted the process (a Rust panic, not a catchable error) on DROP COLUMN in a
// transaction while an older prepared statement was alive. The fixture runs in a child process so
// a regression cannot take the test runner down with it.
describe("Turso DROP COLUMN inside a transaction", () => {
	it.each([
		["sql", "manual"],
		["sql", "owner"],
		["database", "manual"],
		["database", "owner"],
	])("does not abort the process (%s front end, %s mode)", async (frontEnd, mode) => {
		const result = await runFixture([frontEnd, mode]);
		expect(result.stderr).not.toMatch(/panic/i);
		expect(result.code).toBe(0);
		expect(result.stdout).toContain(
			'OK [{"id":1,"active_account_id":"a1","active_org_id":"o1"}]',
		);
	});
});