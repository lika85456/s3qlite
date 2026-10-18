import { Context, Effect, Option } from "effect";

import type { ReplyMode } from "./types";

/**
 * Reply mode override for owner-mode statements run by the current fiber. `None` uses the
 * database default (`syncMode.reply`, or the reply mode of a `DurableEntity` RPC).
 */
export const Current = Context.Reference<Option.Option<ReplyMode>>("@lika85456/s3qlite/ReplyMode", {
	defaultValue: () => Option.none(),
});

/** Runs `effect` with the given reply mode for its owner-mode statements. */
export const withMode: {
	(mode: ReplyMode): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
	<A, E, R>(effect: Effect.Effect<A, E, R>, mode: ReplyMode): Effect.Effect<A, E, R>;
} = ((...args: readonly unknown[]) => {
	if (args.length === 1) {
		const mode = args[0] as ReplyMode;
		return <A, E, R>(effect: Effect.Effect<A, E, R>) =>
			Effect.provideService(effect, Current, Option.some(mode));
	}
	const [effect, mode] = args as [Effect.Effect<unknown, unknown, unknown>, ReplyMode];
	return Effect.provideService(effect, Current, Option.some(mode));
}) as never;

/**
 * Every statement or transaction in `effect` returns only after its changes are pushed and the
 * head compare-and-set succeeded. A failed push fails the statement.
 */
export const durable = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
	withMode(effect, "durable");

/** Statements in `effect` return at once; the background push ships their changes later. */
export const fast = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
	withMode(effect, "fast");