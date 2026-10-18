import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Option } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import { LocalKV, RemoteKV, getJson, headKey, snapshotKey } from "../src/storage";
import type { Head } from "../src/types";

type RemoteKVService = (typeof RemoteKV)["Service"];
type LocalKVService = (typeof LocalKV)["Service"];

const open = (dbName: string, remote: RemoteKVService, local: LocalKVService) =>
	Effect.acquireRelease(
		connect(dbName, { bucket: "test" }).pipe(
			Effect.provideService(RemoteKV, remote),
			Effect.provideService(LocalKV, local),
		),
		(db) => Effect.promise(() => db.close()),
	);

const expectItems = (db: { all: (sql: string) => Promise<unknown[]> }, expected: unknown[]) =>
	Effect.promise(() => db.all("SELECT id FROM items ORDER BY id")).pipe(
		Effect.tap((items) =>
			Effect.sync(() => {
				expect(items).toEqual(expected);
			}),
		),
	);

describe("database creation", () => {
	it.effect("uploads the snapshot before creating the head", () =>
		Effect.gen(function* () {
			const dbName = `create-${crypto.randomUUID().slice(0, 8)}`;
			const remote = yield* makeMemoryKV();
			const events: string[] = [];
			const recordingRemote: RemoteKVService = {
				...remote,
				writeStream: (key, content) =>
					remote
						.writeStream(key, content)
						.pipe(Effect.tap(() => Effect.sync(() => events.push(`upload ${key}`)))),
				create: (key, value) =>
					remote
						.create(key, value)
						.pipe(Effect.tap(() => Effect.sync(() => events.push(`create ${key}`)))),
				set: (key, value) =>
					remote
						.set(key, value)
						.pipe(Effect.tap(() => Effect.sync(() => events.push(`set ${key}`)))),
			};

			yield* open(dbName, recordingRemote, yield* makeMemoryKV());

			const head = yield* getJson<Head>(remote, headKey(dbName)).pipe(
				Effect.orDie,
				Effect.map(Option.getOrThrow),
			);
			const snapshotId = head.value.snapshots[0]?.id ?? "";
			expect(events).toEqual([
				`upload ${snapshotKey(snapshotId)}`,
				`create ${headKey(dbName)}`,
			]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("does not publish a head when the snapshot upload fails", () =>
		Effect.gen(function* () {
			const dbName = `create-${crypto.randomUUID().slice(0, 8)}`;
			const remote = yield* makeMemoryKV();
			const failingRemote: RemoteKVService = {
				...remote,
				writeStream: () => Effect.die(new Error("upload failed")),
			};

			const result = yield* Effect.exit(open(dbName, failingRemote, yield* makeMemoryKV()));
			expect(Exit.isFailure(result)).toBe(true);
			expect(yield* remote.exists(headKey(dbName))).toBe(false);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("lets the loser of a creation race adopt the winner's head", () =>
		Effect.gen(function* () {
			const dbName = `race-${crypto.randomUUID().slice(0, 8)}`;
			const remote = yield* makeMemoryKV();
			const bothUploaded = yield* Deferred.make<boolean>();
			let uploads = 0;
			// Hold every head creation until both creators uploaded their snapshot, so both
			// pass the "no head yet" check and race on `If-None-Match: *`.
			const racingRemote: RemoteKVService = {
				...remote,
				writeStream: (key, content) =>
					remote.writeStream(key, content).pipe(
						Effect.tap(() =>
							Effect.suspend(() => {
								uploads += 1;
								return uploads === 2
									? Deferred.succeed(bothUploaded, true)
									: Effect.void;
							}),
						),
					),
				create: (key, value) =>
					Deferred.await(bothUploaded).pipe(Effect.andThen(remote.create(key, value))),
			};

			const [first, second] = yield* Effect.all(
				[
					open(dbName, racingRemote, yield* makeMemoryKV()),
					open(dbName, racingRemote, yield* makeMemoryKV()),
				],
				{ concurrency: 2 },
			);

			const head = yield* getJson<Head>(remote, headKey(dbName)).pipe(
				Effect.orDie,
				Effect.map(Option.getOrThrow),
			);
			expect(head.value.snapshots).toHaveLength(1);
			// The loser removed its orphaned snapshot.
			const snapshotId = head.value.snapshots[0]?.id ?? "";
			expect(yield* remote.exists(snapshotKey(snapshotId))).toBe(true);

			yield* Effect.promise(() => first.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)"));
			yield* Effect.promise(() => first.run("INSERT INTO items VALUES (1)"));
			yield* first.push();
			yield* second.pull();
			yield* Effect.promise(() => second.run("INSERT INTO items VALUES (2)"));
			yield* second.push();
			yield* first.pull();

			yield* expectItems(first, [{ id: 1 }, { id: 2 }]);
			yield* expectItems(second, [{ id: 1 }, { id: 2 }]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("syncs replicas that connected before the first push", () =>
		Effect.gen(function* () {
			const dbName = `zero-${crypto.randomUUID().slice(0, 8)}`;
			const remote = yield* makeMemoryKV();
			const a = yield* open(dbName, remote, yield* makeMemoryKV());
			const b = yield* open(dbName, remote, yield* makeMemoryKV());
			const c = yield* open(dbName, remote, yield* makeMemoryKV());

			yield* Effect.promise(() => a.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)"));
			yield* Effect.promise(() => a.run("INSERT INTO items VALUES (1)"));
			yield* a.push();

			yield* b.pull();
			yield* expectItems(b, [{ id: 1 }]);

			// `c` writes locally before ever pulling; its change is replayed on top of the batch.
			yield* Effect.promise(() => c.exec("CREATE TABLE IF NOT EXISTS other (id INTEGER)"));
			yield* c.sync();
			yield* a.pull();
			yield* expectItems(c, [{ id: 1 }]);
			expect(
				yield* Effect.promise(() =>
					a.all("SELECT name FROM sqlite_schema WHERE name = 'other'"),
				),
			).toEqual([{ name: "other" }]);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});