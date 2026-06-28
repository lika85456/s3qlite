import { Data, Effect, Option } from "effect";

import { ConnectionConfig } from "./contexts";
import { RemoteKV, headKey } from "./storage";

export class SameNameError extends Data.TaggedError("SameNameError")<{
	readonly dbName: string;
}> {}

export class AlreadyExistsError extends Data.TaggedError("AlreadyExistsError")<{
	readonly dbName: string;
}> {}

export class SourceDoesNotExistError extends Data.TaggedError("SourceDoesNotExistError")<{
	readonly dbName: string;
}> {}

export type ForkError = SameNameError | AlreadyExistsError | SourceDoesNotExistError;

export const fork = (
	nextDbName: string,
): Effect.Effect<void, ForkError, ConnectionConfig | RemoteKV> =>
	Effect.gen(function* () {
		const remoteKV = yield* RemoteKV;
		const { dbName } = yield* ConnectionConfig;

		if (nextDbName === dbName) {
			return yield* Effect.fail(new SameNameError({ dbName }));
		}

		const existingFork = yield* remoteKV.get(headKey(nextDbName));
		if (Option.isSome(existingFork)) {
			return yield* Effect.fail(new AlreadyExistsError({ dbName: nextDbName }));
		}

		const sourceHead = yield* remoteKV.get(headKey(dbName));
		if (Option.isNone(sourceHead)) {
			return yield* Effect.fail(new SourceDoesNotExistError({ dbName }));
		}

		yield* remoteKV.set(headKey(nextDbName), sourceHead.value.value);
	});