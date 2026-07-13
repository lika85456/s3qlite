import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { connect } from "@lika85456/s3qlite/effect";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import { bucket, localDirectory, s3 } from "./config";

describe("Effect API", () => {
	it("runs a scoped database program", async () => {
		const program = Effect.scoped(
			Effect.gen(function* () {
				const db = yield* connect("effect", { bucket, localDirectory });
				yield* Effect.tryPromise(() =>
					db.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)"),
				);
				yield* Effect.tryPromise(() =>
					db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
				);
				yield* db.sync();
				return yield* Effect.tryPromise(
					() => db.all("SELECT * FROM users ORDER BY id") as Promise<unknown[]>,
				);
			}),
		);

		const users = await Effect.runPromise(
			program.pipe(Effect.provide(fileSystemLayer), Effect.provide(S3.layer(s3))),
		);
		expect(users).toEqual([{ id: "1", name: "alice" }]);
	});
});