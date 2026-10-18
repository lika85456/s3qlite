# s3qlite

## 0.4.0-next.0

### Breaking Changes

- Ported to Effect v4: peer dependencies are now `effect@>=4.0.1 <5` and `@effect-aws/client-s3@^2.0.0`
  (`@effect/platform` is no longer needed; `FileSystem` comes from `effect/FileSystem`, the Node
  implementation from `@effect/platform-node@^4`). Effect API users must use Effect v4 too.
- The package is ESM only. The CommonJS build was removed because effect v4 and
  `@effect-aws/client-s3` v2 are ESM only; CommonJS consumers can `require()` the ESM build on
  Node.js 20.19+ / 22.12+.
- `KV` implementations must provide `create` (write only if absent), and `LocalKV` must provide
  `move` and `open`.
- Non-manual sync modes now push unpushed changes when the connection closes. Set
  `pushOnClose: false` to keep the old behavior.
- `fork` and `from` now report `AlreadyExistsError` when the target head is created concurrently.
- `@tursodatabase/database` peer dependency is now `^0.8.3`.
- `@effect/platform-node` moved from `dependencies` to an optional peer dependency
  (`>=4.0.1 <5`, needed by the `/effect` entry point), so apps keep one copy of `effect`. The
  unused `zod` peer dependency was removed.
- `push` (and `S3qliteService.push`) can also fail with `OwnershipLostError`/`PushFailedError`
  (owner mode only).
- `KV` may implement `list(prefix)` (optional; owner-mode GC needs it). `fork` writes a marker
  object `{source}.forks/{target}`.

### Features

- `S3qliteClient.layer(...)`: an Effect `SqlClient` (sqlite dialect, `withTransaction`, savepoints,
  `reserve`) backed by s3qlite, plus the `S3qlite` service for `pull`/`push`/`sync`/`fork`.
  `sqlClientLayer(...)` provides it with Node's file system and an S3 client.
- `pushOnClose` connection option.
- Owner mode, `syncMode: { type: "owner" }`: single writer that restores (or reuses its local
  copy), claims the head with a compare-and-set, never pulls, pushes CDC batches under unique keys
  and is fenced (`OwnershipLostError`) when another instance claims the database. Durable and fast
  reply modes with per-call overrides (`S3qlite.durable`, `S3qlite.fast`, `Reply.withMode`),
  periodic snapshots with head compaction, opt-in garbage collection (fork aware), final push on
  close. `S3qlite.flush`, `status`, `snapshot`, `gc`. See `docs/owner-mode.md`.
- `HeadStore`: keep the authoritative head (and the fencing compare-and-set) in Postgres or another
  SQL database with `HeadStore.layerSql()`; the bucket gets a mirror.
- `Idempotency.once`: apply a write once per write id.
- `@lika85456/s3qlite/cluster`: `DurableEntity.toLayer` gives every Effect cluster entity its own
  owner-mode database as `SqlClient`, with durable or fast replies per entity and per RPC
  (`RpcReplyMode`).

### Fixes

- Pushes prune `turso_cdc` (it grew forever and roughly doubled the database size). With Turso
  0.8.3, `changes` and `lastInsertRowid` of `run()` no longer include the CDC row.
- CDC replay of an UPDATE that changes the primary key (also composite keys) located the row by
  its new key and lost the update; it now uses the before-image. A rowid change (recorded by Turso
  as DELETE + INSERT with the image in `before`) replays too.
- Every prepared statement is closed after use, which avoids a Turso abort on
  `ALTER TABLE ... DROP COLUMN` inside a transaction.
- The Effect SqlClient no longer uses `SqlClient.makeSqliteAcquirers`, which differs in effect
  4.0.1.
- Database names with `/` work with the file KV (parent directories are created).
- Pull publication is recoverable: the old database and connection stay usable until the pull
  commits, the stored head never advances without the database switch, and an interrupted switch
  is finished on the next connect.
- A replica that connected before the first batch can pull it (the snapshot is the rollback base).
- Creation uploads the snapshot before creating the head with `If-None-Match: *`; the loser of a
  creation race adopts the winner's head.
- Pull refuses a remote head that does not extend the local history.
- The file KV `cas` is atomic across processes (lock file), `create` uses an exclusive link, and
  `clone` never leaves a partially written file.
- S3 `If-Match` on a missing key maps to `ConflictError`.

## 0.3.0

### Minor Changes

- CLI & pull, push, sync commands

## 0.2.4

### Patch Changes

- new `from` operation

## 0.2.3

### Patch Changes

- licence and repo link in npm

## 0.2.2

### Patch Changes

- Exports, S3 error typed, docs

## 0.2.1

### Patch Changes

- Added license.

    Added fork to README.

    Added typed error for push to README.

## 0.2.0

### Minor Changes

- Fork operation.

## 0.1.1

### Patch Changes

- better tag names

## 0.1.0

### Minor Changes

- 3ad7258: First publish