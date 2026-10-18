import { Effect, Option } from "effect";

import { ConnectionConfig } from "./contexts";
import { RemoteKV, headKey } from "./storage";
import { AlreadyExistsError, SameNameError, SourceDoesNotExistError } from "./types";
import type { ForkError } from "./types";

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

		// Owner-mode garbage collection of the source keeps objects that forks reference.
		yield* remoteKV.set(`${dbName}.forks/${nextDbName}`, new Uint8Array());
		yield* remoteKV.create(headKey(nextDbName), sourceHead.value.value).pipe(
			Effect.catchTag("ConflictError", () =>
				Effect.fail(
					new AlreadyExistsError({
						dbName: nextDbName,
						message: `Database "${nextDbName}" already exists`,
					}),
				),
			),
		);
	});