import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Option } from "effect";

import { connect } from "../src/connection";
import { makeMemoryKV } from "../src/kv/memoryKV";
import {
	LocalKV,
	RemoteKV,
	dbKey,
	getJson,
	headKey,
	pendingHeadKey,
	stagedDbKey,
	walKey,
} from "../src/storage";
import type { StoredHead } from "../src/types";

type LocalKVService = (typeof LocalKV)["Service"];

type FaultPoint = {
	readonly name: string;
	/** Whether the step runs after the pending head (the commit point) was written. */
	readonly afterCommit: boolean;
	readonly matches: (dbName: string, method: string, args: readonly string[]) => boolean;
};

const faultPoints: readonly FaultPoint[] = [
	{
		name: "rollback base clone",
		afterCommit: false,
		matches: (_dbName, method, [, to]) => method === "clone" && (to ?? "").endsWith(".base"),
	},
	{
		name: "staging the working copy",
		afterCommit: false,
		matches: (dbName, method, [, to]) => method === "move" && to === stagedDbKey(dbName),
	},
	{
		name: "writing the pending head",
		afterCommit: false,
		matches: (dbName, method, [key]) => method === "set" && key === pendingHeadKey(dbName),
	},
	{
		name: "deleting the old WAL",
		afterCommit: true,
		matches: (dbName, method, [key]) => method === "delete" && key === walKey(dbName),
	},
	{
		name: "replacing the live database",
		afterCommit: true,
		matches: (dbName, method, [, to]) => method === "move" && to === dbKey(dbName),
	},
	{
		name: "adopting the pending head",
		afterCommit: true,
		matches: (dbName, method, [, to]) => method === "move" && to === headKey(dbName),
	},
	{
		name: "reopening the live database",
		afterCommit: true,
		matches: (dbName, method, [key]) => method === "open" && key === dbKey(dbName),
	},
];

type FaultMode = "once" | "until-restart";

/**
 * Wraps a LocalKV so the operation selected by `point` dies while `armed` is set. In "once" mode
 * the fault disarms itself after triggering.
 */
const makeFaultyLocal = (inner: LocalKVService, dbName: string, point: FaultPoint) => {
	const control = { armed: false, mode: "once" as FaultMode, triggered: 0 };
	const guard = <A, E, R>(
		method: string,
		args: readonly string[],
		run: () => Effect.Effect<A, E, R>,
	) =>
		Effect.suspend(() => {
			if (!control.armed || !point.matches(dbName, method, args)) {
				return run();
			}
			control.triggered += 1;
			if (control.mode === "once") {
				control.armed = false;
			}
			return Effect.die(new Error(`Injected failure: ${point.name}`));
		});

	const local: LocalKVService = {
		...inner,
		set: (key, value) => guard("set", [key], () => inner.set(key, value)),
		delete: (key) => guard("delete", [key], () => inner.delete(key)),
		clone: (from, to) => guard("clone", [from, to], () => inner.clone(from, to)),
		move: (from, to) => guard("move", [from, to], () => inner.move(from, to)),
		open: (key) => guard("open", [key], () => inner.open(key)),
	};
	return { control, local };
};

const expectItems = (db: { all: (sql: string) => Promise<unknown[]> }, expected: unknown[]) =>
	Effect.promise(() => db.all("SELECT id, value FROM items ORDER BY id")).pipe(
		Effect.tap((items) =>
			Effect.sync(() => {
				expect(items).toEqual(expected);
			}),
		),
	);

/** Remote ETag recorded in the stored head; it changes exactly when a pull is adopted. */
const storedEtag = (local: LocalKVService, dbName: string) =>
	getJson<StoredHead>(local, headKey(dbName)).pipe(
		Effect.orDie,
		Effect.flatMap(
			Option.match({
				onNone: () => Effect.die(new Error("missing stored head")),
				onSome: (head) => Effect.succeed(head.value.remoteEtag),
			}),
		),
	);

describe("pull publication failures", () => {
	for (const point of faultPoints) {
		for (const mode of ["once", "until-restart"] as const) {
			it.effect(`stays consistent when ${point.name} fails (${mode})`, () =>
				Effect.gen(function* () {
					const dbName = `publish-${crypto.randomUUID().slice(0, 8)}`;
					const remote = yield* makeMemoryKV();
					const localA = yield* makeMemoryKV();
					const localB = yield* makeMemoryKV();
					const { control, local: faultyB } = makeFaultyLocal(localB, dbName, point);

					const open = (local: LocalKVService) =>
						Effect.acquireRelease(
							connect(dbName, { bucket: "test" }).pipe(
								Effect.provideService(RemoteKV, remote),
								Effect.provideService(LocalKV, local),
							),
							(db) => Effect.promise(() => db.close()),
						);

					const a = yield* open(localA);
					yield* Effect.promise(() =>
						a.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)"),
					);
					yield* a.push();

					const b = yield* open(faultyB);
					yield* Effect.promise(() => b.run("INSERT INTO items VALUES (2, 'local')"));
					yield* Effect.promise(() => a.run("INSERT INTO items VALUES (1, 'remote')"));
					yield* a.push();
					const etagBefore = yield* storedEtag(localB, dbName);

					control.mode = mode;
					control.armed = true;
					const result = yield* Effect.exit(b.pull());
					control.armed = false;
					expect(control.triggered).toBeGreaterThan(0);

					if (!point.afterCommit) {
						// Before the commit point the pull fails, the stored head is untouched and the
						// old connection keeps working with the old data.
						expect(Exit.isFailure(result)).toBe(true);
						expect(yield* storedEtag(localB, dbName)).toBe(etagBefore);
						expect(yield* localB.exists(pendingHeadKey(dbName))).toBe(false);
						expect(yield* localB.exists(stagedDbKey(dbName))).toBe(false);
						yield* expectItems(b, [{ id: 2, value: "local" }]);
						yield* b.pull();
						yield* expectItems(b, [
							{ id: 1, value: "remote" },
							{ id: 2, value: "local" },
						]);
					} else if (mode === "once") {
						// After the commit point a single failure is rolled forward in-process.
						expect(Exit.isSuccess(result)).toBe(true);
						yield* expectItems(b, [
							{ id: 1, value: "remote" },
							{ id: 2, value: "local" },
						]);
					} else {
						// A persistent failure surfaces; the next connect (a restart) recovers.
						expect(Exit.isFailure(result)).toBe(true);
						const restarted = yield* open(localB);
						expect(yield* localB.exists(pendingHeadKey(dbName))).toBe(false);
						yield* expectItems(restarted, [
							{ id: 1, value: "remote" },
							{ id: 2, value: "local" },
						]);
						yield* restarted.push();
						yield* a.pull();
						yield* expectItems(a, [
							{ id: 1, value: "remote" },
							{ id: 2, value: "local" },
						]);
						return;
					}

					// The local change still reaches the remote exactly once.
					yield* b.push();
					yield* a.pull();
					yield* expectItems(a, [
						{ id: 1, value: "remote" },
						{ id: 2, value: "local" },
					]);
				}).pipe(Effect.provide(fileSystemLayer)),
			);
		}
	}
});