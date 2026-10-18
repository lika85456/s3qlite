import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Option } from "effect";
import { vi } from "vitest";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, getJson, headKey } from "../src/storage";
import type { StoredHead } from "../src/types";

const openDatabase = (
	dbName: string,
	remoteKV: (typeof RemoteKV)["Service"],
	localKV: (typeof LocalKV)["Service"],
	options = {},
) =>
	Effect.acquireRelease(
		connect(dbName, {
			bucket: "test",
			...options,
		}).pipe(Effect.provideService(RemoteKV, remoteKV), Effect.provideService(LocalKV, localKV)),
		(db) => Effect.tryPromise(() => db.close()).pipe(Effect.orDie),
	);

describe("sync modes", () => {
	it.effect("defaults to manual and never syncs implicitly", () =>
		Effect.gen(function* () {
			const dbName = `mode-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const writerLocalKV = yield* makeMemoryKV();
			const readerLocalKV = yield* makeMemoryKV();

			const writer = yield* openDatabase(dbName, remoteKV, writerLocalKV);
			yield* Effect.tryPromise(() =>
				writer.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"),
			);
			yield* writer.push();

			const reader = yield* openDatabase(dbName, remoteKV, readerLocalKV);

			yield* Effect.tryPromise(() =>
				writer.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
			);

			expect(
				yield* Effect.tryPromise(
					() =>
						reader.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				),
			).toEqual([]);

			yield* writer.push();

			expect(
				yield* Effect.tryPromise(
					() =>
						reader.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				),
			).toEqual([]);

			yield* reader.pull();

			expect(
				yield* Effect.tryPromise(
					() =>
						reader.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				),
			).toEqual([{ id: "1", name: "alice" }]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("push-on-change pushes local writes but never pulls implicitly", () =>
		Effect.gen(function* () {
			const dbName = `mode-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const manualLocalKV = yield* makeMemoryKV();
			const autoLocalKV = yield* makeMemoryKV();

			const manual = yield* openDatabase(dbName, remoteKV, manualLocalKV);
			yield* Effect.tryPromise(() =>
				manual.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"),
			);
			yield* manual.push();

			const auto = yield* openDatabase(dbName, remoteKV, autoLocalKV, {
				syncMode: { type: "push-on-change" },
			});

			yield* Effect.tryPromise(() =>
				auto.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
			);

			yield* manual.pull();
			expect(
				yield* Effect.tryPromise(
					() =>
						manual.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				),
			).toEqual([{ id: "1", name: "alice" }]);

			yield* Effect.tryPromise(() =>
				manual.run("INSERT INTO users (id, name) VALUES (?, ?)", "2", "bob"),
			);
			yield* manual.push();

			expect(
				yield* Effect.tryPromise(
					() => auto.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				),
			).toEqual([{ id: "1", name: "alice" }]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("transaction mode pulls before reads and pushes after write changes", () =>
		Effect.gen(function* () {
			const dbName = `mode-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const seedLocalKV = yield* makeMemoryKV();
			const txLocalKV = yield* makeMemoryKV();

			const seed = yield* openDatabase(dbName, remoteKV, seedLocalKV);
			yield* Effect.tryPromise(() =>
				seed.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"),
			);
			yield* seed.push();

			const txDb = yield* openDatabase(dbName, remoteKV, txLocalKV, {
				syncMode: { type: "transaction" },
			});

			yield* Effect.tryPromise(() =>
				seed.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
			);
			yield* seed.push();

			expect(
				yield* Effect.tryPromise(async () => {
					const users: unknown[] = [];
					for await (const user of txDb.iterate(
						"SELECT id, name FROM users ORDER BY id",
					)) {
						users.push(user);
					}
					return users;
				}),
			).toEqual([{ id: "1", name: "alice" }]);
			expect(
				yield* Effect.tryPromise(
					() => txDb.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				),
			).toEqual([{ id: "1", name: "alice" }]);

			yield* Effect.tryPromise(() =>
				txDb.run("INSERT INTO users (id, name) VALUES (?, ?)", "2", "bob"),
			);

			yield* seed.pull();
			expect(
				yield* Effect.tryPromise(
					() => seed.all("SELECT id, name FROM users ORDER BY id") as Promise<unknown[]>,
				),
			).toEqual([
				{ id: "1", name: "alice" },
				{ id: "2", name: "bob" },
			]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.live("interval mode syncs periodically", () =>
		Effect.gen(function* () {
			vi.useFakeTimers();
			yield* Effect.addFinalizer(() => Effect.sync(() => vi.useRealTimers()));

			const dbName = `mode-${crypto.randomUUID().slice(0, 8)}`;
			const remoteKV = yield* makeMemoryKV();
			const writerLocalKV = yield* makeMemoryKV();
			const intervalLocalKV = yield* makeMemoryKV();

			const writer = yield* openDatabase(dbName, remoteKV, writerLocalKV);
			yield* Effect.tryPromise(() =>
				writer.exec("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"),
			);
			yield* writer.push();

			const intervalDb = yield* connect(dbName, {
				bucket: "test",
				syncMode: { type: "interval", everyMs: 25 },
			}).pipe(
				Effect.provideService(RemoteKV, remoteKV),
				Effect.provideService(LocalKV, intervalLocalKV),
			);
			const initialHead = yield* getJson<StoredHead>(intervalLocalKV, headKey(dbName));
			if (Option.isNone(initialHead)) {
				return yield* Effect.fail(new Error("Missing local head for interval database"));
			}

			yield* Effect.tryPromise(() =>
				writer.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
			);
			yield* writer.push();

			yield* Effect.promise(() =>
				vi.waitFor(async () => {
					const syncedHead = await Effect.runPromise(
						getJson<StoredHead>(intervalLocalKV, headKey(dbName)),
					);
					expect(
						Option.isSome(syncedHead) &&
							syncedHead.value.etag !== initialHead.value.etag,
					).toBe(true);
				}),
			);

			yield* Effect.tryPromise(() => intervalDb.close());
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});