import { connect } from "@lika85456/s3qlite";
import { describe, expect, it } from "vitest";

import { bucket, localDirectory, s3 } from "./config";

describe("promise API", () => {
	it("creates, syncs, and reads a database", async () => {
		const db = await connect("promise", { bucket, localDirectory, s3 });
		try {
			await db.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)");
			await db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice");
			await db.sync();
			expect(await db.all("SELECT * FROM users ORDER BY id")).toEqual([
				{ id: "1", name: "alice" },
			]);
		} finally {
			await db.close();
		}
	});
});