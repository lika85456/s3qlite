import { connect } from "@lika85456/s3qlite";
import { describe, expect, it } from "vitest";

import { bucket, localDirectory, s3 } from "./config";

describe("transaction sync mode", () => {
	it("pulls before and pushes after database operations", async () => {
		const db = await connect("transaction", {
			bucket,
			localDirectory,
			s3,
			syncMode: { type: "transaction" },
		});
		try {
			await db.exec("CREATE TABLE events (id TEXT PRIMARY KEY, name TEXT NOT NULL)");
			await db.run("INSERT INTO events (id, name) VALUES (?, ?)", "1", "created");
			expect(await db.all("SELECT * FROM events")).toEqual([{ id: "1", name: "created" }]);
		} finally {
			await db.close();
		}
	});
});