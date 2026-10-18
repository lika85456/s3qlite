import { Schema } from "effect";
import * as ClusterSchema from "effect/cluster/ClusterSchema";
import * as Entity from "effect/cluster/Entity";
import * as Rpc from "effect/rpc/Rpc";

const AppendPayload = {
	/** Client-chosen id; retries reuse it, so the write is applied once. */
	writeId: Schema.String,
	text: Schema.String,
	/** Sleep inside the handler before the write (simulates a long in-flight RPC). */
	delayMs: Schema.Number,
};

export const AppendResult = Schema.Struct({
	/** Rows after the write (as recorded when the write id was first applied). */
	count: Schema.Number,
	/** Runner that applied the write first. */
	appliedBy: Schema.String,
	/** Runner that answered this request. */
	servedBy: Schema.String,
});

/** Volatile write: sent over HTTP only. */
export const Append = Rpc.make("Append", { payload: AppendPayload, success: AppendResult });

/** Durable message: stored in Postgres and redelivered to the next owner after a handoff. */
export const AppendPersisted = Rpc.make("AppendPersisted", {
	payload: AppendPayload,
	success: AppendResult,
}).annotate(ClusterSchema.Persisted, true);

export const Read = Rpc.make("Read", {
	success: Schema.Struct({
		writeIds: Schema.Array(Schema.String),
		servedBy: Schema.String,
	}),
});

export type NotesRpcs = typeof Append | typeof AppendPersisted | typeof Read;

/** Same protocol twice: one entity type replies durably, the other fast. */
export const NotesDurable = Entity.make("NotesDurable", [Append, AppendPersisted, Read]);
export const NotesFast = Entity.make("NotesFast", [Append, AppendPersisted, Read]);

export const entityFor = (reply: string) => (reply === "fast" ? NotesFast : NotesDurable);