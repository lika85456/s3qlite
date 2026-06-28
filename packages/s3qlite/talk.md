## Summary

This file captures the current architecture and API decisions discussed so far for `packages/s3qlite`.

## Purpose

`s3qlite` is a wrapper around the Turso database that syncs database state through S3-compatible storage.

Core user-facing operations:

- `pull`
- `push`
- `sync`

Deferred for later:

- `fork`
- `checkpoint`

## Data Model

Metadata should be inlined instead of split into separate `Meta` types.

```ts
type Snapshot = {
	id: string;
	baseSnapshotId?: string;
	appliedBatchIds: readonly string[];
};

type Batch = {
	id: string;
	fromId: string;
	toId: string;
};

type Head = {
	snapshot: Snapshot;
	batches: readonly Batch[];
};

type StoredHead = {
	head: Head;
	eTag: string | null;
};
```

Notes:

- `StoredHead` does not need `dbName`; file naming already provides that.
- `eTag` is the remote object ETag used for CAS writes.
- `eTag` is not an API version and not a logical incrementing head version.
- `null` means the remote head file does not exist yet.

## Public API

The public wrapper should be `typeof Database & additional operations`.

The user should not get low-level head inspection methods like `getLocalHead` or `getRemoteHead`.

Expected public shape:

```ts
type S3QLite = Database & {
	pull: () => Effect.Effect<PullResult, PullError>;
	push: () => Effect.Effect<PushResult, PushError>;
	sync: () => Effect.Effect<SyncResult, SyncError>;
};
```

Creation options should stay minimal for now:

```ts
type CreateOptions = {
	dbName: string;
	dataDir?: string;
	// syncMode?: ...
	// conflictPolicy?: ...
};
```

Notes:

- This package will be written in Effect.
- Logging should come from an Effect service, not a constructor `logger` option.
- Automatic sync mode should be deferred for now, but likely later supports:
    - manual
    - interval based
    - transactional (before and after user operation)
- Conflict policy should also be deferred for now, but later should support:
    - fail
    - prefer remote
    - prefer local
    - callback-based resolution

## Internal Shape

Avoid class-like or OOP-heavy service wrappers when the logic is mostly thin composition over filesystem and S3 operations.

Prefer standalone functions over large object abstractions.

Examples of the preferred shape:

- `readRemote(key)`
- `writeRemote(key, value, { version })`
- `readLocal(key)`
- `writeLocal(key, value)`
- `syncToLocal(key)`
- `syncToLocal(keys)`
- `cowCopy(from, to)`
- `extractChanges(fromId)`
- `applyChange(...)`
- `replayChanges(...)`
- `applyBatch(db, key)`

Notes:

- `syncToLocal` is a good abstraction for downloading one or more remote objects into the local cache directory.
- Remote CAS writes should likely just be `writeRemote(key, value, { version })`, where `version` is the remote ETag.
- Separate `ObjectRepository`, `Workspace`, or `CdcRuntime` abstractions currently feel unnecessary and too indirect.
- Functions should stay close to the actual algorithm.

## Local State

Internally there are still two different notions of head:

- current remote head
- local `syncedHead`

`syncedHead` is the last successfully synced remote head persisted locally.

This should be named `syncedHead`, not `localHead`, to avoid confusing it with the current live database state which may already contain unsynced local CDC changes.

## Result Types

Result types should use distinct unions rather than optional fields on a single object.

Avoid `_tag`-style variants unless there is a concrete reason to adopt that style in this package.

The exact final result shapes are still open, but the direction is:

- `PullResult` is a union
- `PushResult` is a union
- `SyncResult` is a union

## Pull Behavior

`pull` should be atomic from the user point of view.

High-level algorithm:

1. Gate or queue user database calls through the wrapper.
2. Read local `syncedHead`.
3. Read remote head.
4. If nothing changed remotely, no-op.
5. Extract unsynced local raw CDC events.
6. Copy the local database into a temporary workspace.
7. Apply missing remote batches onto the temporary workspace.
8. Replay local CDC events on top of that temporary workspace.
9. Swap the wrapped live database instance to the new state.
10. Persist the new `syncedHead` locally.
11. Resume queued user operations against the new instance.

Important note:

- The actual swap is delicate because it involves closing the current connection and creating a new one.
- This should stay tightly coupled to the wrapper implementation, not hidden behind a generic workspace abstraction.

If replaying local changes fails, the live database should remain unchanged.

## Push Behavior

High-level algorithm:

1. Gate or queue user database calls through the wrapper.
2. Read local `syncedHead`.
3. Read remote head and its ETag.
4. If remote head differs from `syncedHead`, `push` fails.
5. Extract local raw CDC events.
6. If there are no changes, no-op.
7. Serialize and upload the batch.
8. Build the next head.
9. CAS-write the remote head using the ETag.
10. Persist the new `syncedHead` locally.
11. Resume queued operations.

## Sync Behavior

`sync` should be the smarter operation.

Expected behavior:

- perform pull then push
- if the push loses a CAS race because the remote head changed, retry internally
- retries should be bounded, not infinite

This is the main value of `sync` over manual `pull()` + `push()`.

## CDC

The change unit is raw CDC events.

The CDC stream comes from the Turso database itself through pragma-based APIs.

This needs to be explored directly in the Turso documentation before implementation details are finalized.

## CoW Optimization

When a temporary copy of the database is needed, prefer a filesystem-level CoW copy first.

Desired behavior:

1. attempt `cp` with the CoW/reflink flag
2. if that fails, fall back to normal copy

This should stay as a simple implementation detail, not a large clone-mode abstraction.

## Constraints and Decisions

- One writer per local database.
- Remote head updates must use CAS.
- Current conflict behavior should be fail-only.
- Later conflict resolution should support remote/local preference and callback-based resolution.
- `sync()` should retry internally on head races.
- Errors should emerge naturally from the Effect-based implementation rather than from a big predeclared error union.

## Next Research Targets

- current `packages/s3qlite` codebase structure
- Turso CDC pragma APIs and semantics
- exact wrapper/database swap mechanics needed for atomic pull