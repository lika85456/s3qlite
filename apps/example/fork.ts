import { connect } from "@lika85456/s3qlite";
import { describe, expect, it } from "vitest";

import { bucket, localDirectory, s3 } from "./config";

describe("fork", () => {
	it("creates a database from an existing remote head", async () => {
		const source = await connect("fork-source", { bucket, localDirectory, s3 });
		try {
			await source.exec("CREATE TABLE fork_values (value TEXT PRIMARY KEY)");
			await source.run("INSERT INTO fork_values (value) VALUES (?)", "source");
			await source.sync();
			await source.fork("fork-target");
		} finally {
			await source.close();
		}

		const target = await connect("fork-target", { bucket, localDirectory, s3 });
		try {
			expect(await target.all("SELECT * FROM fork_values")).toEqual([{ value: "source" }]);
		} finally {
			await target.close();
		}
	});
});