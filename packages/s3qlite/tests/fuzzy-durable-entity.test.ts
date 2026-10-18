/**
 * Property-based tests of `DurableEntity`: the pure policy (reply mode resolution, fenced restart
 * delay, default database name) and an in-process entity (`Entity.makeTestClient`) on memory KVs.
 * No container and no network: the entity runs against `/dev/shm` stores.
 */
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { Cause, Effect, Exit, Layer, Option, Result, Schema, Scope } from "effect";
import * as Entity from "effect/cluster/Entity";
import * as ShardingConfig from "effect/cluster/ShardingConfig";
import * as Rpc from "effect/rpc/Rpc";
import { SqlClient } from "effect/sql/SqlClient";
import fc from "fast-check";
import { describe, expect, it } from "vitest";

import * as DurableEntity from "../src/cluster/durableEntity";
import * as Idempotency from "../src/cluster/idempotency";
import { defaultDbName, fencedRestartWaitMs, replyModeOf } from "../src/cluster/policy";
import { makeMemoryKV } from "../src/kv/memoryKV";
import * as S3qliteClient from "../src/sql/client";
import { LocalKV, RemoteKV, headKey } from "../src/storage";
import { OwnershipLostError } from "../src/types";
import type { ReplyMode } from "../src/types";

type KVService = (typeof RemoteKV)["Service"];
type LocalKVService = (typeof LocalKV)["Service"];

const modeArb = fc.constantFrom<ReplyMode>("durable", "fast");

// Keys that exist on every object through `Object.prototype`.
const prototypeKeyArb = fc.constantFrom(
	"toString",
	"constructor",
	"valueOf",
	"hasOwnProperty",
	"__proto__",
);

const tagArb = fc.oneof(fc.string({ maxLength: 8 }), prototypeKeyArb);

// oxlint-disable-next-line local-rules/function-minimum-length -- used by every policy property.
const optionArb = <A>(arb: fc.Arbitrary<A>): fc.Arbitrary<Option.Option<A>> =>
	fc.oneof(fc.constant(Option.none<A>()), arb.map(Option.some));

describe("DurableEntity policy", () => {
	it("resolves rpcReply, then the annotation, then reply, then durable", () => {
		fc.assert(
			fc.property(
				tagArb,
				optionArb(fc.uniqueArray(fc.tuple(tagArb, modeArb), { selector: ([tag]) => tag })),
				optionArb(modeArb),
				optionArb(modeArb),
				(tag, entries, annotation, fallback) => {
					// Oracle on the entry list, independent of property lookup on an object.
					const expected = entries.pipe(
						Option.flatMap((list) =>
							Option.fromNullishOr(list.find(([key]) => key === tag)),
						),
						Option.map(([, mode]) => mode),
						Option.orElse(() => annotation),
						Option.orElse(() => fallback),
						Option.getOrElse((): ReplyMode => "durable"),
					);
					const resolved = replyModeOf(tag, {
						byTag: Option.map(entries, Object.fromEntries),
						annotation,
						fallback,
					});
					expect(resolved).toBe(expected);
				},
			),
			{ numRuns: 1000 },
		);
	});

	it("never resolves an Object.prototype member as a reply mode", () => {
		fc.assert(
			fc.property(prototypeKeyArb, optionArb(modeArb), (tag, fallback) => {
				expect(
					replyModeOf(tag, {
						byTag: Option.some({}),
						annotation: Option.none(),
						fallback,
					}),
				).toBe(Option.getOrElse(fallback, () => "durable"));
			}),
		);
	});

	it("bounds the fenced restart wait by [0, delay] and shrinks it as time passes", () => {
		const timeArb = fc.integer({ min: 0, max: 2 ** 40 });
		const delayArb = fc.integer({ min: 0, max: 10_000_000 });
		fc.assert(
			fc.property(timeArb, timeArb, timeArb, delayArb, (fencedAt, nowA, nowB, delay) => {
				const [earlier, later] = nowA <= nowB ? [nowA, nowB] : [nowB, nowA];
				const waitEarlier = fencedRestartWaitMs(fencedAt, earlier, delay);
				const waitLater = fencedRestartWaitMs(fencedAt, later, delay);
				expect(waitEarlier).toBeGreaterThanOrEqual(0);
				expect(waitEarlier).toBeLessThanOrEqual(delay);
				expect(waitLater).toBeLessThanOrEqual(waitEarlier);
				if (later >= fencedAt) {
					// Normal case: the rest of the delay.
					expect(waitLater).toBe(Math.max(0, delay - (later - fencedAt)));
				}
				if (later >= fencedAt + delay) {
					expect(waitLater).toBe(0);
				}
			}),
			{ numRuns: 1000 },
		);
	});

	it("names databases <entityType>/<entityId>, decodable at the first slash", () => {
		fc.assert(
			fc.property(
				fc.string({ minLength: 1 }).filter((type) => !type.includes("/")),
				fc.string(),
				(entityType, entityId) => {
					const name = defaultDbName({ entityType, entityId });
					const slash = name.indexOf("/");
					expect([name.slice(0, slash), name.slice(slash + 1)]).toEqual([
						entityType,
						entityId,
					]);
				},
			),
		);
	});
});

// --- In-process entity --------------------------------------------------------------------------

const AppendPayload = { writeId: Schema.String };

/** Default reply mode of the entity: durable. */
const Append = Rpc.make("Append", { payload: AppendPayload, success: Schema.Number });
/** Fast through the annotation. */
const AppendFast = Rpc.make("AppendFast", {
	payload: AppendPayload,
	success: Schema.Number,
}).annotate(DurableEntity.RpcReplyMode, "fast");
/** Annotated fast, but `rpcReply` makes it durable (the option wins). */
const AppendOverridden = Rpc.make("AppendOverridden", {
	payload: AppendPayload,
	success: Schema.Number,
}).annotate(DurableEntity.RpcReplyMode, "fast");
/** Unpushed writes of the owner, read without a flush. */
const Pending = Rpc.make("Pending", { success: Schema.Number }).annotate(
	DurableEntity.RpcReplyMode,
	"fast",
);

const Notes = Entity.make("Notes", [Append, AppendFast, AppendOverridden, Pending]);

type AppendTag = "Append" | "AppendFast" | "AppendOverridden";

const modeOfTag: Readonly<Record<AppendTag, ReplyMode>> = {
	Append: "durable",
	AppendFast: "fast",
	AppendOverridden: "durable",
};

/** Appends `writeId` once; the reply is the row count when the id was first applied. */
const append = ({ payload }: { readonly payload: { readonly writeId: string } }) =>
	Idempotency.once(
		{ writeId: payload.writeId, success: Schema.Number },
		Effect.gen(function* () {
			const sql = yield* SqlClient;
			yield* sql`INSERT INTO entries (write_id) VALUES (${payload.writeId})`;
			const [row] = yield* sql<{ readonly n: number }>`SELECT count(*) AS n FROM entries`;
			return row.n;
		}),
	).pipe(Effect.orDie);

const createEntries = Effect.gen(function* () {
	const sql = yield* SqlClient;
	yield* sql`CREATE TABLE IF NOT EXISTS entries (
		id INTEGER PRIMARY KEY,
		write_id TEXT NOT NULL
	)`;
}).pipe(Effect.orDie);

const build = Effect.gen(function* () {
	yield* createEntries;
	const s3qlite = yield* S3qliteClient.S3qlite;
	return Notes.of({
		Append: append,
		AppendFast: append,
		AppendOverridden: append,
		Pending: () =>
			s3qlite.status.pipe(
				Effect.map(Option.match({ onNone: () => -1, onSome: (s) => s.pendingWrites })),
			),
	});
});

const notesLayer = (remote: KVService, local: LocalKVService) =>
	DurableEntity.toLayerFromKV(Notes, build, {
		bucket: "test",
		// Fast writes stay unpushed until a durable reply forces a push.
		pushDelayMs: 60_000,
		snapshot: false,
		rpcReply: { AppendOverridden: "durable" },
	}).pipe(
		Layer.provide(Layer.succeed(RemoteKV, remote)),
		Layer.provide(Layer.succeed(LocalKV, local)),
	);

/** Opens the entity on its own scope (closing it passivates the entity). */
const startEntity = (remote: KVService, entityId: string) =>
	Effect.gen(function* () {
		const scope = yield* Scope.make();
		const makeClient = yield* Entity.makeTestClient(
			Notes,
			notesLayer(remote, yield* makeMemoryKV()),
		).pipe(Scope.provide(scope), Effect.provide(ShardingConfig.layerDefaults));
		return { client: yield* makeClient(entityId), scope };
	});

/** Another owner of `dbName` on `remote`; building it claims the database. */
const ownerLayer = (remote: KVService, dbName: string) =>
	S3qliteClient.layerFromKV({ dbName, bucket: "test", syncMode: { type: "owner" } }).pipe(
		Layer.provide(Layer.succeed(RemoteKV, remote)),
		Layer.provide(Layer.effect(LocalKV, makeMemoryKV())),
		Layer.orDie,
	);

const readWriteIds = Effect.gen(function* () {
	yield* createEntries;
	const sql = yield* SqlClient;
	const rows = yield* sql<{
		readonly write_id: string;
	}>`SELECT write_id FROM entries ORDER BY id`;
	return rows.map((row) => row.write_id);
}).pipe(Effect.orDie);

/**
 * Copies the bucket as it is now. The head is read first: objects are immutable and uploaded
 * before the head that references them, so the copy is a consistent published state.
 */
const copyBucket = (remote: KVService, dbName: string) =>
	Effect.gen(function* () {
		const copy = yield* makeMemoryKV();
		const head = Option.getOrThrow(yield* remote.get(headKey(dbName)));
		const objects = yield* Option.fromNullishOr(remote.list).pipe(Option.getOrThrow, (list) =>
			list(""),
		);
		for (const { key } of objects) {
			if (key !== headKey(dbName)) {
				const object = Option.getOrThrow(yield* remote.get(key));
				yield* copy.set(key, object.value);
			}
		}
		yield* copy.set(headKey(dbName), head.value);
		return copy;
	});

type Op = { readonly tag: AppendTag; readonly writeId: string };

const opArb: fc.Arbitrary<Op> = fc.record({
	tag: fc.constantFrom<AppendTag>("Append", "AppendFast", "AppendOverridden"),
	// A small pool, so retries of the same write id are common.
	writeId: fc.constantFrom("w0", "w1", "w2", "w3"),
});

/** Model of the entity: applied write ids in order, and whether unpushed writes exist. */
type Model = { readonly applied: string[]; dirty: boolean; durable: readonly string[] };

const expectedReply = (model: Model, writeId: string): number => {
	const index = model.applied.indexOf(writeId);
	if (index >= 0) {
		return index + 1;
	}
	model.applied.push(writeId);
	return model.applied.length;
};

/** Runs `ops` against the entity and checks every reply and the published state. */
const runOps = (
	remote: KVService,
	dbName: string,
	client: Effect.Success<ReturnType<typeof startEntity>>["client"],
	ops: readonly Op[],
) =>
	Effect.gen(function* () {
		// The build creates the `entries` table: an unpushed write.
		const model: Model = { applied: [], dirty: true, durable: [] };
		for (const op of ops) {
			const isNew = !model.applied.includes(op.writeId);
			const reply = yield* client[op.tag]({ writeId: op.writeId });
			// Idempotent retries: a reused write id gets the reply of its first application.
			expect(reply).toBe(expectedReply(model, op.writeId));
			if (modeOfTag[op.tag] === "durable") {
				// A durable reply pushed every write so far, including earlier fast ones.
				model.dirty = false;
				model.durable = [...model.applied];
				expect(yield* client.Pending()).toBe(0);
				// Restore a copy of the bucket, so the check does not fence the live owner.
				const copy = yield* copyBucket(remote, dbName);
				expect(yield* Effect.provide(readWriteIds, ownerLayer(copy, dbName))).toEqual(
					model.applied,
				);
			} else {
				// A fast reply does not push; a retry of an applied id writes nothing.
				model.dirty ||= isNew;
				expect((yield* client.Pending()) > 0).toBe(model.dirty);
			}
		}
		return model;
	});

const runEntityCase = (ops: readonly Op[]) =>
	Effect.gen(function* () {
		const remote = yield* makeMemoryKV();
		const entityId = crypto.randomUUID();
		const dbName = defaultDbName({ entityType: Notes.type, entityId });
		const { client, scope } = yield* startEntity(remote, entityId);
		const model = yield* runOps(remote, dbName, client, ops);
		// Passivation pushes the rest: every applied write is published exactly once.
		yield* Scope.close(scope, Exit.void);
		expect(yield* Effect.provide(readWriteIds, ownerLayer(remote, dbName))).toEqual(
			model.applied,
		);
	});

const runFencingCase = (ops: readonly Op[]) =>
	Effect.gen(function* () {
		const remote = yield* makeMemoryKV();
		const entityId = crypto.randomUUID();
		const dbName = defaultDbName({ entityType: Notes.type, entityId });
		const { client, scope } = yield* startEntity(remote, entityId);
		const model = yield* runOps(remote, dbName, client, ops);
		// Another runner claims the database and keeps it open.
		const claimed = yield* Scope.make();
		const claimant = yield* Layer.buildWithScope(ownerLayer(remote, dbName), claimed);
		// The old instance cannot acknowledge a durable write any more: it dies fenced.
		const late = yield* Effect.exit(client.Append({ writeId: "late" }));
		const defect = Exit.isFailure(late)
			? Result.getSuccess(Cause.findDefect(late.cause))
			: Option.none();
		expect(Option.exists(defect, (error) => error instanceof OwnershipLostError)).toBe(true);
		yield* Scope.close(scope, Exit.void);
		// The claimant has every durably acknowledged write, nothing it never saw published, and
		// nothing the fenced instance wrote after the claim.
		const restored = yield* Effect.provideContext(readWriteIds, claimant);
		expect(restored.slice(0, model.durable.length)).toEqual(model.durable);
		expect(model.applied.slice(0, restored.length)).toEqual(restored);
		expect(restored).not.toContain("late");
		yield* Scope.close(claimed, Exit.void);
	});

describe("DurableEntity in process", () => {
	it("replies match the reply mode and idempotent retries; durable replies are published", async () => {
		await fc.assert(
			fc.asyncProperty(fc.array(opArb, { minLength: 1, maxLength: 8 }), (ops) =>
				Effect.runPromise(
					runEntityCase(ops).pipe(Effect.scoped, Effect.provide(fileSystemLayer)),
				),
			),
			{ numRuns: 15 },
		);
	});

	it("a claim by another owner fences the entity without losing durable replies", async () => {
		await fc.assert(
			fc.asyncProperty(fc.array(opArb, { maxLength: 6 }), (ops) =>
				Effect.runPromise(
					runFencingCase(ops).pipe(Effect.scoped, Effect.provide(fileSystemLayer)),
				),
			),
			{ numRuns: 10 },
		);
	});
});