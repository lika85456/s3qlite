import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

// turso_cdc used to grow forever (and roughly doubled the database size).
describe("CDC table growth", () => {
	it.effect("stays bounded over many pushes in push-on-change mode", () =>
		Effect.gen(function* () {
			const remote = yield* makeMemoryKV();
			const local = yield* makeMemoryKV();
			const database = yield* connect(`growth-${crypto.randomUUID()}`, {
				bucket: "test",
				syncMode: { type: "push-on-change" },
			}).pipe(Effect.provideService(RemoteKV, remote), Effect.provideService(LocalKV, local));

			yield* Effect.promise(() =>
				database.exec("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"),
			);
			for (let index = 0; index < 300; index += 1) {
				yield* Effect.promise(() =>
					database.run("INSERT INTO notes (body) VALUES (?)", `note ${index}`),
				);
			}

			// One row stays: the newest pushed change keeps change ids growing.
			const [cdc] = yield* Effect.promise(
				() =>
					database.all("SELECT count(*) AS n FROM turso_cdc") as Promise<{ n: number }[]>,
			);
			expect(cdc.n).toBeLessThanOrEqual(2);
			const result = yield* Effect.promise(() =>
				database.run("UPDATE notes SET body = 'x' WHERE id <= 5"),
			);
			expect(result.changes).toBe(5);
			const inserted = yield* Effect.promise(() =>
				database.run("INSERT INTO notes (body) VALUES ('last')"),
			);
			expect(inserted.lastInsertRowid).toBe(301);
		}).pipe(Effect.scoped, Effect.provide(fileSystemLayer)),
	);
});