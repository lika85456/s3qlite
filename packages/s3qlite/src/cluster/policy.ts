/**
 * Pure decisions of `DurableEntity`. They live apart from the cluster wiring so tests can check
 * them without a cluster. Not part of the public `@lika85456/s3qlite/cluster` API.
 */
import { Option } from "effect";

import type { ReplyMode } from "../types";

export type EntityAddress = {
	readonly entityType: string;
	readonly entityId: string;
};

/** Database name of an entity when `options.dbName` is not set. */
export const defaultDbName = ({ entityType, entityId }: EntityAddress): string =>
	`${entityType}/${entityId}`;

export type ReplyModeSources = {
	/** `options.rpcReply`. */
	readonly byTag: Option.Option<Readonly<Partial<Record<string, ReplyMode>>>>;
	/** The `RpcReplyMode` annotation of the RPC. */
	readonly annotation: Option.Option<ReplyMode>;
	/** `options.reply`. */
	readonly fallback: Option.Option<ReplyMode>;
};

/**
 * Reply mode of the RPC `tag`: `rpcReply[tag]`, then the `RpcReplyMode` annotation, then
 * `options.reply`, then `durable`.
 */
export const replyModeOf = (tag: string, sources: ReplyModeSources): ReplyMode =>
	sources.byTag.pipe(
		// Own keys only: a tag such as "toString" must not resolve to an `Object.prototype` member,
		// which is not "durable" and would silently skip the durable flush.
		Option.flatMap((byTag) =>
			Object.hasOwn(byTag, tag) ? Option.fromNullishOr(byTag[tag]) : Option.none(),
		),
		Option.orElse(() => sources.annotation),
		Option.orElse(() => sources.fallback),
		Option.getOrElse((): ReplyMode => "durable"),
	);

/**
 * How long a rebuild still waits after this runner saw the entity fenced at `fencedAt`. Clamped to
 * `[0, delayMs]`, so a wall clock that moved backwards cannot extend the wait.
 */
export const fencedRestartWaitMs = (fencedAt: number, now: number, delayMs: number): number =>
	Math.min(delayMs, Math.max(0, delayMs - (now - fencedAt)));