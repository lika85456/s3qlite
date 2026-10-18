import { Effect } from "effect";

import { ConnectionState } from "./contexts";
import { LocalKV, dbKey, headKey, pendingHeadKey, shmKey, stagedDbKey, walKey } from "./storage";

/**
 * Finishes or discards an interrupted pull publication.
 *
 * A pull first builds `{db}.db.staged` (complete and checkpointed), then writes
 * `{db}.json.pending`. The pending head is the commit point:
 *
 * - pending + staged: the live database was not replaced yet, so replace it and adopt the head;
 * - pending only: the database was replaced, so only the head is left to adopt;
 * - staged only: the pull never committed, so the staged copy is discarded.
 *
 * Every step is an atomic rename or an idempotent delete, so recovery can itself be interrupted
 * and repeated. The live database must not be open while this runs.
 */
export const recoverPublication = (dbName: string): Effect.Effect<boolean, never, LocalKV> =>
	Effect.gen(function* () {
		const local = yield* LocalKV;
		const pending = yield* local.exists(pendingHeadKey(dbName));
		const staged = yield* local.exists(stagedDbKey(dbName));

		if (!pending) {
			if (staged) {
				yield* local.delete(stagedDbKey(dbName));
			}
			return false;
		}

		if (staged) {
			// The staged copy already contains every change from the old WAL, see `pull`.
			yield* local.delete(walKey(dbName));
			yield* local.delete(shmKey(dbName));
			yield* local.move(stagedDbKey(dbName), dbKey(dbName));
		}

		yield* local.move(pendingHeadKey(dbName), headKey(dbName));
		yield* Effect.logInfo("s3qlite.publication.recovered").pipe(
			Effect.annotateLogs({ dbName }),
		);
		return true;
	});

/**
 * Replaces the live database with the staged copy and adopts the pending head.
 *
 * The old connection stays open until the pull is committed. Once the staged copy and pending head
 * exist, a failure in any later step is rolled forward by `recoverPublication` and a fresh
 * connection is opened. Only when that recovery also fails does this fail; the next `connect`
 * retries the recovery.
 */
export const publishStaged = (
	dbName: string,
): Effect.Effect<void, Error, LocalKV | ConnectionState> =>
	Effect.gen(function* () {
		const local = yield* LocalKV;
		const state = yield* ConnectionState;
		const oldConnection = yield* state.getConnection;

		yield* Effect.promise(() => oldConnection.close()).pipe(Effect.ignore);

		const switchFiles = Effect.gen(function* () {
			yield* local.delete(walKey(dbName));
			yield* local.delete(shmKey(dbName));
			yield* local.move(stagedDbKey(dbName), dbKey(dbName));
			yield* local.move(pendingHeadKey(dbName), headKey(dbName));
		});

		const reopen = local.open(dbKey(dbName)).pipe(Effect.flatMap(state.setConnection));

		yield* switchFiles.pipe(
			Effect.andThen(reopen),
			Effect.catchCause((cause) =>
				Effect.logWarning("s3qlite.publication.retry").pipe(
					Effect.annotateLogs({ dbName, cause: String(cause) }),
					Effect.andThen(recoverPublication(dbName)),
					Effect.andThen(reopen),
				),
			),
			Effect.catchCause((cause) =>
				Effect.fail(
					new Error(
						`Failed to publish pulled database "${dbName}"; it will be recovered on the next connect: ${String(cause)}`,
					),
				),
			),
		);
	});