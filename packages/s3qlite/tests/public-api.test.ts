import { describe, expect, it } from "@effect/vitest";

import * as cluster from "../cluster";
import * as effectApi from "../effect";
import * as promiseApi from "../index";

const effectExports = [
	"AlreadyExistsError",
	"BucketDoesNotExistError",
	"ConflictError",
	"FileNotFoundError",
	"HeadStore",
	"OwnershipLostError",
	"PushFailedError",
	"ReplayError",
	"Reply",
	"S3qlite",
	"S3qliteClient",
	"SameNameError",
	"SourceDoesNotExistError",
	"SyncModeError",
	"connect",
	"from",
	"ownerErrorOf",
];

// Pins the runtime exports, so internals (CDC, KV, storage format) do not leak back in through
// `export *`. Changing this list changes the public API.
describe("public API", () => {
	it("effect entry point exports only the public values", () => {
		expect(Object.keys(effectApi).sort()).toEqual([...effectExports].sort());
	});

	it("root entry point adds the Promise helpers", () => {
		expect(Object.keys(promiseApi).sort()).toEqual([...effectExports, "sqlClientLayer"].sort());
	});

	it("S3qliteClient namespace exposes the layer and its services only", () => {
		expect(Object.keys(effectApi.S3qliteClient).sort()).toEqual([
			"S3qlite",
			"S3qliteClient",
			"layer",
		]);
	});

	it("cluster entry point exports DurableEntity and Idempotency", () => {
		expect(Object.keys(cluster).sort()).toEqual(["DurableEntity", "Idempotency"]);
		expect(Object.keys(cluster.DurableEntity).sort()).toEqual(["RpcReplyMode", "toLayer"]);
		expect(Object.keys(cluster.Idempotency).sort()).toEqual([
			"ensureTable",
			"isApplied",
			"once",
			"prune",
			"tableName",
		]);
	});
});