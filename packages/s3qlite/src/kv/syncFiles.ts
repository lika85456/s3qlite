import { Effect, Option } from "effect";

import { LocalKV, RemoteKV } from "../storage";

export const pullFiles = (keys: string[]) =>
	Effect.forEach(keys, (key) =>
		Effect.gen(function* () {
			const localKV = yield* LocalKV;
			const remoteKV = yield* RemoteKV;

			if (yield* localKV.exists(key)) {
				return;
			}

			const readStream = yield* remoteKV.readStream(key);

			if (Option.isNone(readStream)) {
				return yield* Effect.fail(
					new Error(`File ${key} does not exist remotely or cannot be read`),
				);
			}

			yield* localKV.writeStream(key, readStream.value);
		}),
	);

export const pushFiles = (keys: string[]) =>
	Effect.forEach(keys, (key) =>
		Effect.gen(function* () {
			const localKV = yield* LocalKV;
			const remoteKV = yield* RemoteKV;

			if (yield* remoteKV.exists(key)) {
				return;
			}

			const readStream = yield* localKV.readStream(key);

			if (Option.isNone(readStream)) {
				return yield* Effect.fail(
					new Error(`File ${key} does not exist locally or cannot be read`),
				);
			}

			yield* remoteKV.writeStream(key, readStream.value);
		}),
	);