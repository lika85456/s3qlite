import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/** Root folder for files the test suites create (state files, local databases, logs). */
export const testTmpDir = join(tmpdir(), "s3qlite-tests");

mkdirSync(testTmpDir, { recursive: true });

/** Path inside {@link testTmpDir}. */
export const testTmpPath = (...segments: readonly string[]): string =>
	join(testTmpDir, ...segments);