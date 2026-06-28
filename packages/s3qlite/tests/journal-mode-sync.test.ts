import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { expect } from "@effect/vitest";
import { Effect } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV } from "../src/storage";

import { makeRustfsRemoteKV, rustfs } from "./utils/rustfs";

// ponytail: library assumes WAL journal mode for wal_checkpoint(TRUNCATE).
// turso/libSQL defaults to WAL; this test guards against silent regressions.

rustfs("journal-mode-sync", (it) => {
	it.effect("default journal mode is WAL and wal_checkpoint(TRUNCATE) succeeds", () =>
		Effect.scoped(
			Effect.gen(function* () {
				const dbName = `jm-${crypto.randomUUID().slice(0, 8)}`;
				const remoteKV = yield* makeRustfsRemoteKV("journal-mode");
				const localKV = yield* makeMemoryKV();

				const db = yield* connect(dbName, { bucket: "test" }).pipe(
					Effect.provideService(RemoteKV, remoteKV),
					Effect.provideService(LocalKV, localKV),
				);

				// Must be WAL — the library calls wal_checkpoint(TRUNCATE) in
				// connection, push, and pull paths. A different mode would
				// silently no-op or error.
				const [{ journal_mode }] = yield* Effect.tryPromise(
					() => db.all("PRAGMA journal_mode") as Promise<[{ journal_mode: string }]>,
				);
				expect(journal_mode).toBe("wal");

				// Verify the truncate pragma works without throwing.
				yield* Effect.tryPromise(() => db.run("PRAGMA wal_checkpoint(TRUNCATE)"));

				// After checkpoint the database is still usable.
				yield* Effect.tryPromise(() => db.exec("CREATE TABLE t (x INTEGER)"));
				yield* Effect.tryPromise(() => db.run("INSERT INTO t (x) VALUES (1)"));
				const rows = yield* Effect.tryPromise(
					() => db.all("SELECT x FROM t") as Promise<{ x: number }[]>,
				);
				expect(rows).toEqual([{ x: 1 }]);
			}).pipe(Effect.provide(fileSystemLayer)),
		),
	);
});