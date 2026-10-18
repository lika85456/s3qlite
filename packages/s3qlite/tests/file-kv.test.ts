import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Option } from "effect";

import { makeMemoryKV } from "../src/kv/memoryKV";

const textEncoder = new TextEncoder();

describe("file kv", () => {
	it.effect("lets exactly one of many concurrent CAS writers win", () =>
		Effect.gen(function* () {
			const kv = yield* makeMemoryKV();
			const { etag } = yield* kv.set("head.json", textEncoder.encode("initial"));

			const results = yield* Effect.all(
				Array.from({ length: 16 }, (_, index) =>
					Effect.exit(kv.cas("head.json", textEncoder.encode(`writer-${index}`), etag)),
				),
				{ concurrency: "unbounded" },
			);

			expect(results.filter(Exit.isSuccess)).toHaveLength(1);
			expect(results.filter(Exit.isFailure)).toHaveLength(15);
		}).pipe(Effect.provide(fileSystemLayer)),
	);

	it.effect("lets exactly one of many concurrent creators win", () =>
		Effect.gen(function* () {
			const kv = yield* makeMemoryKV();

			const results = yield* Effect.all(
				Array.from({ length: 16 }, (_, index) =>
					Effect.exit(kv.create("new.json", textEncoder.encode(`creator-${index}`))),
				),
				{ concurrency: "unbounded" },
			);

			expect(results.filter(Exit.isSuccess)).toHaveLength(1);
			const stored = yield* kv.get("new.json").pipe(Effect.map(Option.getOrThrow));
			expect(new TextDecoder().decode(stored.value)).toMatch(/^creator-\d+$/);
		}).pipe(Effect.provide(fileSystemLayer)),
	);
});