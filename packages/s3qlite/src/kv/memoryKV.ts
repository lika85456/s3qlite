import { FileSystem } from "@effect/platform/FileSystem";
import { Effect } from "effect";
import type { Scope } from "effect";

import { makeFileKV } from "./fileKV";
import type { CloneTrait, ConnectDatabaseTrait, KV } from "./kv";

export const makeMemoryKV = (): Effect.Effect<
	KV & CloneTrait & ConnectDatabaseTrait,
	never,
	FileSystem | Scope.Scope
> =>
	Effect.gen(function* () {
		const fs = yield* FileSystem;
		const rootDirectory = `/dev/shm/s3qlite-${crypto.randomUUID()}`;

		yield* Effect.logDebug(`memoryKV.makeMemoryKV(${rootDirectory})`);
		yield* fs.makeDirectory(rootDirectory, { recursive: true }).pipe(Effect.orDie);
		yield* Effect.addFinalizer(() =>
			fs.remove(rootDirectory, { recursive: true, force: true }).pipe(Effect.orDie),
		);

		return yield* makeFileKV(rootDirectory);
	});