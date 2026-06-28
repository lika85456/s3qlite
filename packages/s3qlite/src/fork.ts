import { Data, Effect, Option } from "effect";

import { ConnectionConfig } from "./contexts";
import { RemoteKV, headKey } from "./storage";

export class SameNameError extends Data.TaggedError("SameNameError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

export class AlreadyExistsError extends Data.TaggedError("AlreadyExistsError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

export class SourceDoesNotExistError extends Data.TaggedError("SourceDoesNotExistError")<{
	readonly dbName: string;
	readonly message: string;
}> {}

export type ForkError = SameNameError | AlreadyExistsError | SourceDoesNotExistError;

export const fork = (
	nextDbName: string,
): Effect.Effect<void, ForkError, ConnectionConfig | RemoteKV> =>
	Effect.gen(function* () {
		const remoteKV = yield* RemoteKV;
		const { dbName } = yield* ConnectionConfig;

		if (nextDbName === dbName) {
			return yield* Effect.fail(
				new SameNameError({
					dbName,
					message: `Cannot fork database "${dbName}" into itself`,
				}),
			);
		}

		const existingFork = yield* remoteKV.get(headKey(nextDbName));
		if (Option.isSome(existingFork)) {
			return yield* Effect.fail(
				new AlreadyExistsError({
					dbName: nextDbName,
					message: `Database "${nextDbName}" already exists`,
				}),
			);
		}

		const sourceHead = yield* remoteKV.get(headKey(dbName));
		if (Option.isNone(sourceHead)) {
			return yield* Effect.fail(
				new SourceDoesNotExistError({
					dbName,
					message: `Source database "${dbName}" does not exist`,
				}),
			);
		}

		yield* remoteKV.set(headKey(nextDbName), sourceHead.value.value);
	});
