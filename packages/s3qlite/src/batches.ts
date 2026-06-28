import type { Database } from "@tursodatabase/database";
import { Effect, Option } from "effect";
import { v4 } from "uuid";

import { replayCDC } from "./cdc/apply";
import { extractCDC } from "./cdc/extract";
import { deserializeCDC, serializeCDC } from "./cdc/protobuf";
import { LocalKV, batchKey } from "./storage";
import type { Batch } from "./types";

export const applyBatch = (database: Database, batch: Batch): Effect.Effect<void, never, LocalKV> =>
	Effect.gen(function* () {
		const localKv = yield* LocalKV;
		const deserialized = deserializeCDC(
			yield* localKv.get(batchKey(batch.id)).pipe(
				Effect.flatMap(
					Option.match({
						onNone: () => Effect.die(new Error(`Missing local batch ${batch.id}`)),
						onSome: ({ value }) => Effect.succeed(value),
					}),
				),
			),
		);
		yield* replayCDC(database, deserialized).pipe(Effect.orDie);
	});

export type ExtractedBatch = {
	batch: Batch;
	lastLocalChangeId: number;
};

export const extractBatch = (
	database: Database,
	lastChangeId: number,
): Effect.Effect<Option.Option<ExtractedBatch>, never, LocalKV> =>
	Effect.gen(function* () {
		const localKV = yield* LocalKV;
		const changes = yield* extractCDC(database, lastChangeId);

		if (changes.length === 0) {
			return Option.none();
		}

		const firstChange = changes[0];
		const lastChange = changes[changes.length - 1];

		if (!firstChange || !lastChange) {
			return yield* Effect.die(new Error("Unexpected empty changes array"));
		}

		const id = v4();
		const serialized = serializeCDC(changes);
		yield* localKV.set(batchKey(id), serialized);

		return Option.some<ExtractedBatch>({
			batch: { id },
			lastLocalChangeId: lastChange.changeId,
		});
	});
