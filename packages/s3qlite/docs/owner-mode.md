# Owner mode (design)

Status: implemented in `src/owner/` (engine), `src/sql/client.ts` (reply gate),
`src/cluster/durableEntity.ts` (Effect cluster integration) and `src/idempotency.ts`.

## Goal

One instance at a time is the only writer of a database (one per user, tenant or agent
session). It writes at local speed, never pulls, ships its changes to S3, and no second writer
can corrupt the remote history, including a writer that was paused and resumed.

## Data layout

| Key                                | Written by                | Notes                                                                                                                   |
| ---------------------------------- | ------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `{dbName}.json`                    | create, claim, every push | The head. `generation` counts claims, `owner` is diagnostics. With an external head store this is a best-effort mirror. |
| `{dbName}.objects/{uuid}.batch`    | push                      | One serialized CDC batch. Unique key per upload; never overwritten.                                                     |
| `{dbName}.objects/{uuid}.snapshot` | create, snapshot          | Database file with an empty `turso_cdc`.                                                                                |
| `{source}.forks/{target}`          | `fork`                    | Empty marker so GC of `source` keeps what forks reference.                                                              |

The batch and snapshot ids include the prefix (`{dbName}.objects/{uuid}`), so the existing
`batchKey(id)` / `snapshotKey(id)` mapping and the classic pull/join code keep working.

Locally (`localDirectory`): `{dbName}.db` (+ WAL), `{dbName}.json` (stored head: head, version,
`lastSyncedLocalChangeId`). Batch and snapshot files are deleted after upload.

## Lifecycle

### Open and claim (`openAndClaim`)

1. Finish an interrupted pull publication (`recoverPublication`), in case the directory was used
   by another mode before.
2. Read the head and its version (S3 ETag, or the generation of an external store). With an
   external store and no row yet, import `{dbName}.json` from the bucket.
3. No head: create the database (snapshot first, then the head with create-if-absent). Losing
   the race deletes the uploaded snapshot and goes to 2.
4. **Cache check**: when the local stored head has the same version as the remote head and the
   database file exists, reuse it. Otherwise restore: download the latest snapshot and the
   batches after it, replay them, and drop the downloaded files.
5. **Claim**: compare-and-set the head to `{ ...head, generation: generation + 1, owner }`. On
   conflict go to 2 (at most 5 times).
6. Prune CDC rows up to `lastSyncedLocalChangeId` (a previous life may have crashed between the
   head write and the prune). If CDC rows newer than that exist, they are writes the previous
   life on this machine committed but did not push; they extend the claimed head, so they are
   pushed by the first round.

The claim is the important part. In Effect cluster tests, a runner that was SIGSTOPped
resumed after another runner took over. Without the claim, the new owner still held the old
version until its first push, so the stale runner's final push won the compare-and-set and 80
acknowledged writes disappeared. With the claim, every older holder of the head fails its next
compare-and-set.

### Writes

Statements and transactions run under the engine semaphore. `beforeWork` reads
`MAX(change_id)` and fails when the instance is fenced or closed; `afterWork` reads it again. A
different value means the unit wrote: `writeSeq` grows and the pusher wakes up.

### Push rounds (`round`)

One round at a time (`roundLock`), uninterruptible once started:

1. Holding the permit: extract CDC rows newer than `lastSyncedLocalChangeId` into a batch file
   (and, for a snapshot round, `wal_checkpoint(TRUNCATE)` and copy the database file).
   `covered = writeSeq` at this moment.
2. Without the permit (statements continue): upload the batch with a plain PUT under its unique
   key, upload the snapshot (after deleting the copy's CDC rows), compute the next head.
3. Compare-and-set the head with the version from the last successful round.
4. Holding the permit: write the local stored head, then prune `turso_cdc`.
5. `pushedSeq = covered` and signal every waiter.

**CDC pruning** keeps the newest pushed row (`DELETE ... WHERE change_id < lastPushed`): Turso
restarts change ids at 1 once `turso_cdc` is empty (its `AUTOINCREMENT` is not honored), and both
extraction and `lastSyncedLocalChangeId` rely on ids that only grow. The classic push prunes the
same way.

**Lost compare-and-set responses.** When a round fails after its compare-and-set started (for
example a network error after S3 applied it), the round is "uncertain". The next round first
reads the head: if it is exactly the head the uncertain round wrote (unique object keys make this
unambiguous), the round is adopted as successful; if the head still has the old version, nothing
landed and the round is retried; otherwise the instance is fenced.

**Failures.** A compare-and-set conflict fences the instance (`OwnershipLostError`): every later
statement fails, waiters fail, the pusher stops, the local copy is not published. Any other
failure is `PushFailedError`: waiters covered by that round fail, and the pusher retries with
backoff (100 ms doubling up to 5 s) while writes are unpushed.

### Reply modes and the reply gate

- `fast`: a unit returns as soon as it committed locally. The pusher waits `pushDelayMs` (default
  100 ms) after the first wake-up to coalesce writes. **Loss window**: on a crash or hang of the
  owner, writes acknowledged during the last `pushDelayMs` plus one push (one PUT + one
  compare-and-set; about 50 ms against local RustFS, up to 185 ms observed under load) are lost, unless the same machine restarts in the
  same local directory before another owner claims the database (then they are pushed on open).
- `durable`: after the permit is released, the unit waits until `pushedSeq >= writeSeq` as read at
  that moment (`awaitDurable`), waking the pusher without the coalescing delay. Concurrent units
  share rounds (group commit). The target includes writes of other fibers, so a durable reply
  never exposes data that could still be lost (an output gate, like Cloudflare Durable Objects).

The mode comes from `Reply.Current` (a `Context.Reference`, set by `Reply.durable`,
`Reply.fast`, `Reply.withMode`, `S3qlite.durable`, `S3qlite.fast`), falling back to
`syncMode.reply`. The SqlClient gate runs after single statements (borrower) and after the
outermost `withTransaction`. Streams and `reserve` do not gate; use `S3qlite.flush`.

A durable unit that fails with `PushFailedError` has an unknown outcome: it is committed locally
and the retrying pusher may still publish it. The answer is idempotent writes (below), not a local
rollback.

### Snapshots, compaction and restore time

After a successful round, when the batches after the latest snapshot reach
`snapshot.everyBatches` (default 64) or the bytes pushed since reach `snapshot.everyBytes`
(default 64 MiB), the pusher runs a snapshot round. Its head keeps only the new snapshot
(`baseSnapshotId` points to the previous one) and the last batch, which the snapshot lists in
`batchIdsApplied` so that restore knows where to continue. Restore time is therefore bounded by
one snapshot download plus at most `everyBatches` batches. Durable waiters of writes that land
during the snapshot round wait for the snapshot upload too.

### Garbage collection

Opt-in (`gc: { minAgeMs }`, default age 1 hour), runs after a snapshot round or on
`S3qlite.gc`. It lists `{dbName}.objects/`, keeps everything the current head references plus
everything the heads of forks reference (following `{name}.forks/` markers recursively), and
deletes the rest that is older than `minAgeMs`. The age protects objects a newer owner uploaded
but has not referenced yet, and restores of an older head that are still running. The listing
needs `KV.list` (implemented for S3 and the file KV).

### Close

The handler or layer scope closes: stop the pusher (a running round finishes), run up to three
final rounds until one succeeds (skipped when fenced), close the connection. Ownership is not
released in the head: the next owner claims anyway, and a release write would add a round trip to
every handoff without making it safer. The local copy stays, so the next open on this machine can
skip the download.

## Fencing assumptions

The fence is the compare-and-set of the head:

- **Default (`remoteHeadStore`)**: S3 conditional writes on `{dbName}.json`
  (`If-None-Match: *`, `If-Match: <etag>`). Correct only if conditional writes on one key are
  linearizable. True for AWS S3, Cloudflare R2 and single-endpoint MinIO/RustFS. Not true for
  every multi-endpoint S3-compatible deployment (RustFS issue #1659: conditional writes are not
  atomic across endpoints). ETags are content hashes, so every head written by an owner must be
  unique; it is, because the claim increments `generation` and batch ids are UUIDs.
- **External (`HeadStore.layerSql`)**: the head lives in a SQL table (`s3qlite_heads(db_name,
generation, head)`), the compare-and-set is `UPDATE ... WHERE generation = $expected`, and the
  version is the generation. The owner mirrors each new head to `{dbName}.json` after the
  compare-and-set (best effort; readers can lag). Objects stay in the bucket, and since their keys
  are unique, no conditional write on the object store is needed at all.

The cluster's own single-writer guarantee (shard locks) is a lease: it can be violated for a short
time after a hang (the resumed runner keeps serving until its next lock refresh). The claim plus
the compare-and-set turn that window into failed writes instead of lost data.

## Idempotent writes

`Idempotency.once({ writeId, success }, effect)` runs `effect` in a transaction unless `writeId`
is in `s3qlite_applied_requests`; it records the id and the JSON-encoded result in the same
transaction. Retries (client timeouts, `Interrupt` of volatile RPCs at shutdown, redelivered
persisted messages, `PushFailedError`) then apply the write once and get the original reply. The
table moves with the database to the next owner. `Idempotency.prune(maxAgeMs)` keeps it small.

## DurableEntity

`DurableEntity.toLayer(entity, build, options)` wraps `Entity.toLayer`:

- Activation (build effect): wait `fencedRestartDelay` if this runner saw this database fenced
  recently, open and claim the database (`<entityType>/<entityId>`) in the handler scope with
  statement replies `fast`, log `s3qlite.entity.activated` (`downloaded`, `created`, `openMs`,
  `generation`), then run the user's build with `SqlClient`, `S3qlite` and `S3qliteClient`.
- Each handler result (an `Effect`, a `Stream`, or an `Rpc.Wrapper` of either) gets the database
  services. For effects, after the handler finishes (success or failure), a `durable` RPC waits for
  `flush`. If the flush fails, or the instance is fenced, the handler dies: the entity manager
  rebuilds the entity (reopen + claim) and replays the request.
- Reply mode per RPC: `options.rpcReply[tag]`, then the `RpcReplyMode` annotation on the `Rpc`,
  then `options.reply` (default `durable`).
- Passivation, shard release, rebalance and shutdown close the handler scope: final push, close,
  `s3qlite.entity.closed`.

Why the restart delay: when a stale runner resumes, its instance gets fenced (another runner
claimed), dies and would be rebuilt on the same runner, whose claim would fence the legitimate
owner. Waiting at least one shard lock refresh lets the stale runner notice the lost locks first
(the shard is interrupted and the rebuild never happens). A legitimate owner that was fenced by a
stale claim loses availability for the delay and then reclaims.

## Cluster findings applied

From Effect cluster experiments and the cluster suite:

- `shardLockDisableAdvisory: true`; advisory locks never expire while a hung process keeps its TCP
  connection.
- Same `shardLockExpiration` on clients and runners.
- Start runners one at a time on an empty database.
- `regclass` codec for `@effect/sql-pg` release candidates (4.0.5 has it).
- Pin `@effect/platform-node-shared` to the `effect` version.
- The final push is not bounded by `entityTerminationTimeout`, only by the platform grace period.
- Persisted messages are redelivered to the new owner; long volatile ones end with `Interrupt`, so
  callers retry and writes must be idempotent.

## Measured

See the README table and `s3qlite-tests/s3qlite-cluster-logs/results.json` in the system temp folder after
`bun run test:cluster`. Durable replies lost no acknowledged write in any scenario; fast replies
lost 1–5 writes per hang or crash, all acknowledged at most 185 ms before the fault.

## Limits and open points

- The reply gate covers single statements and outermost transactions; streams and `reserve` need
  an explicit `S3qlite.flush`.
- A snapshot round re-uploads the whole database file. Very large databases want page-level
  shipping instead.
- Readers that are not the owner (classic `pull`) cannot follow a compacted head (`pull` refuses a
  head whose snapshot changed); they must re-open.
- `fork` of an owner database flushes first and copies the bucket head; with an external head
  store it copies the mirror.