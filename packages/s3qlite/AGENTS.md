# Project overview

This package is a simple wrapper over the tursodatabase. What it does is it enables clients to sync the database into s3 compatible remote storage and it also provides API to handle pulling, pushing or forking the database.

It is not yet fully implemented and the code is currently very experimental.

It is built on Effect v4. The peer range is `effect >=4.0.1 <5` (consumers may pin
`effect@4.0.1`); the repo develops on 4.0.5. Do not use APIs added after
4.0.1 (for example `SqlClient.makeSqliteAcquirers` has no `borrower` in 4.0.1). Check with the
override procedure in "Testing". Services use `Context.Service`, the SQL client lives in
`effect/sql`, the cluster in `effect/cluster`, and the package is ESM only.

# Architecture

- `src/engine.ts` – the database core (`S3qliteEngine`): bootstrap, the live connection, the
  semaphore that serializes statements with pull/push, the sync policy hooks (`beforeWork`,
  `afterWork`, `around`, `replyGate`), background interval sync and `close` (final push, close
  connection). `syncMode: { type: "owner" }` hands over to `src/owner/owner.ts`.
- `src/owner/owner.ts` – owner mode (single writer): open + claim, push rounds, fencing, reply
  gate, snapshots, GC, close. `src/owner/heads.ts` – object keys, head compaction, CDC pruning.
  `src/owner/headStore.ts` – `HeadStore` (where the fencing compare-and-set happens: S3 head by
  default, SQL table with `HeadStore.layerSql`). Design: `docs/owner-mode.md`.
- `src/replyMode.ts` – `Reply.Current` reference and `Reply.durable/fast/withMode`.
- Public API: `src/index.ts` (root and `/effect`), `src/sql/public.ts` (`S3qliteClient`
  namespace) and `src/cluster/public.ts` + `cluster.ts` (`/cluster`) list every export explicitly.
  Do not add `export *` of internal modules; `tests/public-api.test.ts` pins the list.
- `src/cluster/idempotency.ts` – `Idempotency.once`, exported from `/cluster` (write ids in `s3qlite_applied_requests`).
- `src/cluster/durableEntity.ts` – `DurableEntity.toLayer` for Effect cluster entities, exported
  as `@lika85456/s3qlite/cluster` (`cluster.ts`). Its pure decisions (reply mode resolution, fenced
  restart wait, default database name) are in `src/cluster/policy.ts` (internal; property-tested
  in `tests/fuzzy-durable-entity.test.ts`, with an in-process entity on memory KVs).
- `src/connection.ts` – front end that exposes the engine as a Turso `Database` (`connect`).
- `src/sql/client.ts` – front end that exposes the engine as an Effect `SqlClient`
  (`S3qliteClient.layer`, `S3qlite` sync-control service).
- `src/bootstrap.ts` – create (snapshot first, then head with `If-None-Match: *`) or join a
  database; finishes interrupted pulls first.
- `src/pull.ts`, `src/publication.ts` – pull and its recoverable publication protocol.
- `src/push.ts` – push (head CAS with `If-Match`), then prunes pushed `turso_cdc` rows.

# Data primitives

## Snapshot

A snapshot is a fully working database file. It is a single file, so upon creation from a live database the TRUNCATE operation must be called to truncate the WAL changes. If a snapshot was created from a checkpoint operation, then the metadata of the snapshot will contain the previous snapshot and the batches applied to it.

## Batch

Batch is a collection of changes, currently in the form of CDC list. For transport and storage a protobuf is used. Batches have metadata containing the `fromId` and `toId` which represents the ids of the CDC changes included in the batch (inclusive).

## Head

A head is a pointer to a database state, it is represented by a snapshot and a list of batches.

# Operations

Except normal SQL operations and what the @tursodatabase/database provides, this library wraps the Database class and provides additional operations.

- pull: It pulls the new batches from remote and tries to apply them on a rollbacked version of the clients local database (the base copy of the last batch, or the snapshot when there is no batch yet). Only after the new batches are applied successfully, the local changes are replayed on top and the local database is switched to this new state. The switch is recoverable: the complete replacement is staged as `{dbName}.db.staged`, then `{dbName}.json.pending` is written (the commit point), then the live database and the stored head are replaced with atomic renames. A failure before the commit point leaves the old database and connection untouched; after it, the switch is rolled forward in-process or on the next connect (`recoverPublication`). Pull refuses a remote head whose batch list does not extend the local one.

- push: It bundles the local changes into a batch and tries to push it to the remote. If the remote has a newer head than local state, then the operation fails.

- sync: it does pull & push in sequence and tries to resolve any issues a manual pull and then push could create (like a head changing mid push).

- fork: creates a fork of a already existing database without cloning data, but rather just cloning the head. (Won't implement yet).

- checkpoint: creates a new snapshot from the current state of the database and pushes the change to the remote. (Won't implement yet).

- from: a standalone function (not a method on the database instance) that imports an existing local SQLite database file into s3qlite. It copies the file into the local KV store, truncates the WAL, creates a snapshot and head, and uploads them to S3. After import, the database is accessible via `connect()`. Usage: `from(pathToFile, dbName, connectionOptions)`. Errors: `AlreadyExistsError` (target dbName already taken), `FileNotFoundError` (source file doesn't exist).

During pull/push operations the library halts the clients possible concurrent requests to the database and queues them until the operation is finished.

# Testing strategy

A fast-check library is used on top of vitest to generate random sequences of operations and test the consistency of the database state after applying them.

# S3 and local files structure

The bucket provided is expected to have the following structure:

- `{dbName}.json` - head file
- `{uuid}.batch` - batch file
- `{uuid}.snapshot` - snapshot file
- `{dbName}.objects/{uuid}.batch|.snapshot` - objects uploaded by owner mode (unique keys, GC
  scope)
- `{source}.forks/{target}` - fork marker (owner-mode GC keeps objects that forks reference)

The local directory (by default `./.s3qlite/`) is expected to have the following structure:

- `{dbName}.json` - last synced remote head file
- `{uuid}.batch` - batch file
- `{uuid}.snapshot` - snapshot file
- `{dbName}.db|.wal|.shm` - database files created by tursodatabase.
- `{snapshotId}-{batchId}.base` - rollback base: snapshot plus batches up to `batchId`, without local changes.
- `{dbName}.db.staged`, `{dbName}.json.pending` - in-flight pull publication, see pull above.
- `*.lock` - lock files that serialize compare-and-swap writes of the file KV.

# Testing

- `bun run test` (integration + fuzzy, local RustFS container) and `bun run test:cluster`
  (`vitest.cluster.config.ts`: multi-process Effect cluster with local Postgres + RustFS
  containers, about 4 minutes). Never point tests at remote storage. Do not run the
  `apps/example` tests or the root `turbo test` (it includes them): the example's `.env` points to
  a real bucket and the suite wipes it.
- Owner-mode tests need `it.live`: the pusher sleeps, and `it.effect` freezes the clock.
- Turso can abort the whole process (Rust panic). Reproduce such bugs in a child process
  (`tests/fixtures/`), as `turso-drop-column-panic.test.ts` does.
- Effect 4.0.1 check: set root `overrides` for `effect`, `@effect/platform-node`,
  `@effect/platform-node-shared`, `@effect/platform-bun`, `@effect/vitest`, `@effect/sql-pg` to
  `4.0.1`, `bun install`, run check-types and both suites, then restore `package.json` and
  `bun.lock`.

# Known issues

- Turso 0.8: an UPDATE of a rowid is recorded as DELETE + INSERT whose row image is in `before`
  (extraction normalizes it). `turso_cdc` ids restart at 1 once the table is empty, so pruning
  keeps the newest pushed row.
- Turso panics on `ALTER TABLE ... DROP COLUMN` in a transaction while an older prepared
  statement is alive. Close every statement you prepare.

- Colliding primary-key changes currently resolve with a last-push-wins strategy during sync. If a different merge strategy is needed, add an explicit conflict-resolution option instead of changing the default implicitly.