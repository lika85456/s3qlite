In-process SQL database with sub-millisecond latency persisted on S3 for durability and easy distribution.

<div align="center">
	<img src="./apps/docs/public/s3qlite_logo.png" width="400" alt="S3Qlite" />
</div>

This library provides a wrapper over [Turso database](https://turso.tech/) extending it with additional methods for synchronization.

Tired of not paying a single cent with the generous free tier of Turso Cloud? Run your own!

# Installation

`bun add @lika85456/s3qlite effect@^4 @effect-aws/client-s3@^2 @tursodatabase/database@^0.8.3`

S3Qlite is built on [Effect v4](https://effect.website) and works with any `effect` from 4.0.1 up
to (not including) 5 (tested with 4.0.1 and 4.0.5). Install exactly one copy of `effect` in your
app: with two copies, service lookups fail. The `/effect` entry point also needs
`@effect/platform-node` of the same version as `effect`. If you use cluster packages
(`@effect/platform-bun`, `@effect/platform-node`), pin `@effect/platform-node-shared` to your
`effect` version too (`overrides` in `package.json`), because their `^` ranges pull a newer one.

S3Qlite ships as ESM only, like Effect v4 itself. CommonJS projects can still `require()` it on
Node.js 20.19+ / 22.12+.

## CLI

Manage S3Qlite databases and run SQL from your shell.

Install it globally:

```sh
bun add -g @lika85456/s3qlite-cli
```

Configure your S3-compatible storage once:

```sh
s3qlite alias set local http://localhost:9000 ACCESS_KEY SECRET_KEY --path on
```

For AWS S3, use its regional endpoint:

```sh
s3qlite alias set aws https://s3.us-east-1.amazonaws.com AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
```

Then create a database and use it:

```sh
s3qlite create local/databases/customers
s3qlite sql local/databases/customers \
	'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
s3qlite sql local/databases/customers \
	"INSERT INTO users (name) VALUES ('Alice')"
s3qlite sql local/databases/customers \
	'SELECT id, name FROM users ORDER BY id'
```

List, fork, or remove databases:

```sh
s3qlite ls local/databases
s3qlite fork local/databases/customers local/databases/customers-preview
s3qlite fork local/databases/customers aws/s3qlite-backups/customers
s3qlite remove local/databases/customers-preview --force
```

Database targets use `ALIAS/BUCKET/DATABASE`; `ls` uses `ALIAS/BUCKET`. Use `alias list` to
see configured storage and `alias remove NAME` to delete an alias. Omit credentials from
`alias set` to enter them securely at a prompt. Fork targets can use any configured alias and
bucket.

You can also pipe SQL:

```sh
printf 'SELECT count(*) AS count FROM users' | s3qlite sql local/databases/customers
```

Query results are printed as JSON. Run `s3qlite --help` for all commands and options.

# Usage

## Promise API

```ts
import { connect } from "@lika85456/s3qlite";

const db = await connect(
	"agent-123", // non-existing database will be created on first sync
	{
		bucket: "s3qlite", // single bucket can store multiple databases
		localDirectory: "./.s3qlite", // the local database, batches and other files need to be stored in localDirectory
	},
);

await db.exec(`
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL
	)
`);

await db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice");

// Downloads remote changes and uploads local changes.
await db.sync();

const users = await db.all("SELECT * FROM users ORDER BY id");
console.log(users);

await db.close();
```

## Sync Modes

`connect()` accepts a `syncMode` option that controls automatic synchronization.

```ts
const db = await connect("app-123", {
	bucket: "s3qlite",
	syncMode: "manual",
});
```

Available modes:

- `"manual"` (default): no automatic sync. Only `pull()`, `push()`, and `sync()` talk to remote.
- `{ type: "interval", everyMs: 5000 }`: calls `sync()` periodically in the background.
- `{ type: "transaction" }`: pulls before every database call, then pushes after it if the operation introduced new local changes.
- `{ type: "push-on-change" }`: never pulls automatically, but pushes after a operation if it introduced new local changes.
- `{ type: "owner", ... }`: single-writer mode, see [Owner mode](#owner-mode-single-writer).

`pushOnClose` (default: `true` for every mode except `"manual"`) syncs unpushed local changes
when the connection closes (`db.close()`, scope close or layer release).

Examples:

```ts
await connect("manual-db", {
	bucket: "s3qlite",
	syncMode: "manual",
});

await connect("interval-db", {
	bucket: "s3qlite",
	syncMode: { type: "interval", everyMs: 5_000 },
});

await connect("transaction-db", {
	bucket: "s3qlite",
	syncMode: { type: "transaction" },
});

await connect("push-db", {
	bucket: "s3qlite",
	syncMode: { type: "push-on-change" },
});
```

## Effect API

```ts
import { connect } from "@lika85456/s3qlite/effect";
import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { Effect, Layer, ManagedRuntime } from "effect";

const program = Effect.scoped(
	Effect.gen(function* () {
		const db = yield* connect("user-123", {
			bucket: "s3qlite",
			localDirectory: "./.s3qlite",
		});

		yield* Effect.tryPromise(() =>
			db.exec(`
				CREATE TABLE IF NOT EXISTS users (
					id TEXT PRIMARY KEY,
					name TEXT NOT NULL
				)
			`),
		);

		yield* Effect.tryPromise(() =>
			db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice"),
		);

		yield* db.sync();

		const users = yield* Effect.tryPromise(
			() => db.all("SELECT * FROM users ORDER BY id") as Promise<unknown[]>,
		);

		console.log(users);
	}),
);

const runtime = ManagedRuntime.make(Layer.mergeAll(fileSystemLayer, S3.layer({})));

await runtime.runPromise(program);
```

## Effect SqlClient

S3Qlite can back the standard Effect SQL client (`effect/sql`), so code written against
`SqlClient` (including drizzle's Effect SQLite driver) runs on an S3-synced database. The client
uses the SQLite dialect, `BEGIN IMMEDIATE` transactions with savepoints for nesting, and one
serialized connection. Pulls and pushes queue behind running statements and transactions.

```ts
import { S3qlite, S3qliteClient } from "@lika85456/s3qlite/effect";
import { S3 } from "@effect-aws/client-s3";
import { layer as fileSystemLayer } from "@effect/platform-node/NodeFileSystem";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

const DatabaseLive = S3qliteClient.layer({
	dbName: "app-123",
	bucket: "s3qlite",
	localDirectory: "./.s3qlite",
	syncMode: { type: "push-on-change" },
}).pipe(Layer.provide([fileSystemLayer, S3.layer({})]));

const program = Effect.gen(function* () {
	const sql = yield* SqlClient;
	yield* sql`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)`;
	yield* sql.withTransaction(sql`INSERT INTO users ${sql.insert({ id: "1", name: "alice" })}`);

	// Explicit sync control lives on the S3qlite service.
	const s3qlite = yield* S3qlite;
	yield* s3qlite.sync;

	return yield* sql`SELECT * FROM users`;
});

await Effect.runPromise(program.pipe(Effect.provide(DatabaseLive)));
```

The layer provides `SqlClient.SqlClient`, `S3qliteClient.S3qliteClient` and `S3qlite`
(`pull`, `push`, `sync`, `fork`). It opens (or creates) the database when built and, when its
scope closes, pushes unpushed changes (see `pushOnClose`) and closes the connection.
`sqlClientLayer({ ..., s3 })` from the root entry point is the same layer with Node's file
system and an `S3` client already provided. To hand it to an app that expects
`Layer<SqlClient>` without errors, use
`layer.pipe(Layer.orDie)`.

The sync mode applies to every statement outside a transaction and to every transaction or
reserved connection as a whole: `"transaction"` pulls before it starts and pushes after it
commits.

## Owner mode (single writer)

Owner mode is for a database that has exactly one writer at a time, for example one per user,
tenant or agent session, placed by a scheduler or by Effect cluster. The owner never pulls, so
it writes at local speed, and every push is fenced: once another instance claims the database,
the old owner's next push fails and the old owner stops.

```ts
const DatabaseLive = S3qliteClient.layer({
	dbName: "user/123",
	bucket: "s3qlite",
	localDirectory: "./.s3qlite",
	syncMode: {
		type: "owner",
		reply: "durable", // default; or "fast"
		pushDelayMs: 100, // fast replies: coalescing window before a push
		snapshot: { everyBatches: 64 }, // snapshot + head compaction; `false` disables it
		gc: { minAgeMs: 60 * 60 * 1000 }, // delete unreferenced objects after a snapshot (opt-in)
	},
}).pipe(Layer.provide([fileSystemLayer, S3.layer({})]));
```

What happens:

1. **Open** restores the latest snapshot and the batches after it. When the local copy is at the
   remote head version (same machine, previous owner was this directory), nothing is
   downloaded.
2. **Claim**: before it serves anything, the owner writes the head with `generation + 1`
   (compare-and-set). Any older holder of the head loses its next compare-and-set. Without this
   claim, a resumed stale owner can overwrite newer data (a cluster test lost 80 acknowledged
   writes that way).
3. **Writes** commit locally. A background pusher uploads the new CDC rows as a batch under a
   unique key (never overwriting an object) and compare-and-sets the head. The pushed rows are
   pruned from `turso_cdc`.
4. **Fencing**: a lost compare-and-set fences the instance. Every later statement fails with a
   `SqlError` whose cause is `OwnershipLostError` (use `ownerErrorOf(error)`), and nothing it
   wrote locally after its last push is published.
5. **Snapshots** keep restores bounded: every `snapshot.everyBatches` batches (or
   `everyBytes`), a round uploads the database file and compacts the head to one snapshot.
6. **Close** (scope close, layer release) runs a final push, then closes the connection. The
   local copy stays for the next open.

`pull()` and `sync()` fail with `SyncModeError` in owner mode.

### Reply modes

| Mode | A write returns | Loss on crash or hang of the owner |
| --- | --- | --- |
| `durable` | after its batch is uploaded and the head compare-and-set succeeded | none; a failed push fails the write |
| `fast` | at once | writes acknowledged during the last `pushDelayMs` + one push (about 50–200 ms) |

The database default is `syncMode.reply`. Override it per call:

```ts
import { Reply, S3qlite } from "@lika85456/s3qlite";

yield* S3qlite.durable(sql`INSERT INTO payments ${sql.insert(payment)}`); // wait for S3
yield* S3qlite.fast(sql`UPDATE sessions SET seen_at = ${now}`); // do not wait
yield* Reply.withMode(program, "durable"); // a whole program
```

A durable reply waits for every change committed before it, not only its own (it is an
output gate), so a reply never shows data that could still be lost. Transactions wait once,
after `COMMIT`. `S3qlite.flush` is an explicit barrier. A durable write that fails with
`PushFailedError` (for example S3 is down) has an **unknown outcome**: it is committed locally
and a later push may still publish it. Retry it with the same write id.

`S3qlite.status` returns the generation, the number of unpushed writes and how the database
was opened (`downloaded`, `created`, `openMs`). `S3qlite.snapshot` and `S3qlite.gc` run a
snapshot or a garbage collection now.

### Idempotent writes

Retries happen: a reply can be lost, a handoff interrupts an RPC, a durable message is
delivered again. Give each logical write a stable id and wrap it in `Idempotency.once`. The id
is recorded in `s3qlite_applied_requests` in the same transaction as the write; a retry returns
the stored result without running the write again.

```ts
import { Idempotency } from "@lika85456/s3qlite";

const addItem = (writeId: string, item: Item) =>
	Idempotency.once(
		{ writeId, success: Schema.Struct({ count: Schema.Number }) },
		Effect.gen(function* () {
			const sql = yield* SqlClient;
			yield* sql`INSERT INTO items ${sql.insert(item)}`;
			const [row] = yield* sql<{ n: number }>`SELECT count(*) AS n FROM items`;
			return { count: row.n };
		}),
	);
```

`Idempotency.prune(maxAgeMs)` deletes old records; keep them longer than any retry can arrive.

### Fencing assumptions

The default fence is the S3 conditional write on `{dbName}.json` (`If-None-Match: *` to create,
`If-Match: <etag>` to update). It is safe when conditional writes on one key are linearizable:
AWS S3, Cloudflare R2, a single MinIO or RustFS endpoint. It is **not** safe when the same
bucket is served by several endpoints that do not agree on conditional writes (see
[RustFS issue #1659](https://github.com/rustfs/rustfs/issues/1659)). Batch and snapshot objects
always have unique keys, so the data objects never depend on conditional writes.

To move the compare-and-set out of the object store, provide a `HeadStore`. `HeadStore.layerSql()`
keeps the authoritative head in a SQL table (`s3qlite_heads`, tested with Postgres) and the owner
mirrors the head to `{dbName}.json` for readers, the CLI and forks:

```ts
import { HeadStore } from "@lika85456/s3qlite";
import * as PgClient from "@effect/sql-pg/PgClient";

const DatabaseLive = S3qliteClient.layer({ dbName, bucket, syncMode: { type: "owner" } }).pipe(
	Layer.provide(HeadStore.layerSql().pipe(Layer.provide(PgClient.layer({ url })))),
	Layer.provide([fileSystemLayer, S3.layer({})]),
);
```

A database that exists only in the bucket is imported into the head store on its first owner
open.

### Garbage collection

Owner-mode objects live under `{dbName}.objects/`. After a snapshot, objects there that the head
does not reference and that are older than `gc.minAgeMs` are deleted. The minimum age protects
uploads of a newer owner that its head does not reference yet, and readers that restore an older
head. `fork` writes a marker under `{source}.forks/`, and GC keeps every object that a fork (or a
fork of a fork) references. GC is off by default.

## Durable entities on Effect cluster

`@lika85456/s3qlite/cluster` turns an Effect cluster `Entity` into a self-hosted "Durable
Object": one entity instance per id, placed by the cluster, each with its own owner-mode
database (`<entityType>/<entityId>`) exposed as `SqlClient`.

```ts
// protocol.ts: shared by runners and clients
import { Schema } from "effect";
import * as ClusterSchema from "effect/cluster/ClusterSchema";
import * as Entity from "effect/cluster/Entity";
import * as Rpc from "effect/rpc/Rpc";
import { DurableEntity } from "@lika85456/s3qlite/cluster";

export const AddItem = Rpc.make("AddItem", {
	payload: { writeId: Schema.String, name: Schema.String },
	success: Schema.Struct({ count: Schema.Number }),
}).annotate(ClusterSchema.Persisted, true); // redelivered to the next owner after a handoff

export const ListItems = Rpc.make("ListItems", {
	success: Schema.Array(Schema.String),
}).annotate(DurableEntity.RpcReplyMode, "fast"); // per-RPC reply mode

export const Cart = Entity.make("Cart", [AddItem, ListItems]);
```

```ts
// runner.ts
import { S3 } from "@effect-aws/client-s3";
import * as BunClusterHttp from "@effect/platform-bun/BunClusterHttp";
import * as BunFileSystem from "@effect/platform-bun/BunFileSystem";
import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import * as PgClient from "@effect/sql-pg/PgClient";
import { Effect, Layer, Option, Redacted, Schema } from "effect";
import * as RunnerAddress from "effect/cluster/RunnerAddress";
import { SqlClient } from "effect/sql/SqlClient";
import { Idempotency } from "@lika85456/s3qlite";
import { DurableEntity } from "@lika85456/s3qlite/cluster";
import { AddItem, Cart } from "./protocol";

const CartLive = DurableEntity.toLayer(
	Cart,
	Effect.gen(function* () {
		// Runs on every activation, after the database is open and claimed.
		const sql = yield* SqlClient;
		yield* sql`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`.pipe(
			Effect.orDie,
		);
		return Cart.of({
			AddItem: ({ payload }) =>
				Idempotency.once(
					{ writeId: payload.writeId, success: AddItem.successSchema },
					Effect.gen(function* () {
						yield* sql`INSERT INTO items (name) VALUES (${payload.name})`;
						const [row] = yield* sql<{ n: number }>`SELECT count(*) AS n FROM items`;
						return { count: row.n };
					}),
				).pipe(Effect.orDie),
			ListItems: () =>
				sql<{ name: string }>`SELECT name FROM items ORDER BY id`.pipe(
					Effect.map((rows) => rows.map((row) => row.name)),
					Effect.orDie,
				),
		});
	}),
	{
		bucket: "carts",
		localDirectory: "/var/lib/carts", // a persistent volume lets reactivations skip the download
		reply: "durable", // entity default; RPCs override with RpcReplyMode or `rpcReply`
		maxIdleTime: "5 minutes", // passivation: final push, then close
		snapshot: { everyBatches: 64 },
	},
);

const postgres = PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) });

const ClusterLive = BunClusterHttp.layer({
	transport: "http",
	storage: "sql",
	shardingConfig: {
		runnerAddress: Option.some(RunnerAddress.make(process.env.POD_IP!, 34431)),
		runnerListenAddress: Option.some(RunnerAddress.make("0.0.0.0", 34431)),
		shardLockDisableAdvisory: true,
		shardLockExpiration: "10 seconds",
		shardLockRefreshInterval: "3 seconds",
		entityTerminationTimeout: "10 seconds",
	},
}).pipe(Layer.provide(postgres));

CartLive.pipe(
	Layer.provide([S3.layer({}), BunFileSystem.layer]),
	Layer.provide(ClusterLive),
	Layer.launch,
	BunRuntime.runMain,
);
```

```ts
// anywhere with a cluster client (same shardLockExpiration as the runners)
const carts = yield* Cart.client;
const { count } = yield* carts("cart-42").AddItem({ writeId: crypto.randomUUID(), name: "apple" });
```

Lifecycle: activation opens and claims the database, then runs your build effect; idle
passivation, shard release, rebalance and shutdown close the handler scope, which pushes the
remaining changes and closes the database. A durable RPC replies after its changes (and every
change before them) are pushed. When the instance finds itself fenced, it dies on purpose: the
cluster rebuilds it (reopen + claim) and replays the request. The rebuild first waits
`fencedRestartDelay` (default 10 s), so a runner that resumed after a hang notices that it lost
its shard locks before it could claim the database back. Handlers also get `S3qlite` (status,
flush, `S3qlite.durable(...)`) and `S3qliteClient`. Use `DurableEntity.toLayerFromKV` to bring
your own `LocalKV`/`RemoteKV`.

### Cluster settings that matter

- `shardLockDisableAdvisory: true`. With the default Postgres advisory locks, a hung
  (SIGSTOPped) runner keeps its locks and nobody takes over; with table locks the takeover takes
  about `shardLockExpiration`.
- Clients and runners need the same `shardLockExpiration`; clients use it to decide which
  runners are alive.
- Start runners one at a time on an empty database (or run the cluster migrations first): a
  concurrent first start can crash the losers.
- `@effect/sql-pg` 4.0.0 release candidates cannot decode `regclass`, which breaks the second
  start; 4.0.5 has the codec. For older versions register one (see
  `packages/s3qlite/tests/cluster/pg-types.ts`).
- The final push runs in a scope finalizer, which `entityTerminationTimeout` does **not** bound
  (it only bounds in-flight handlers). Only the platform does: set Kubernetes
  `terminationGracePeriodSeconds` above `entityTerminationTimeout` plus the worst-case final
  push of all resident entities. A SIGKILL at the end of the grace period is a crash.
- Volatile RPCs that are still running at shutdown end with `Interrupt`, and callers retry.
  Persisted (`ClusterSchema.Persisted`) messages are delivered again to the next owner. Both
  mean a write can arrive twice: use write ids.

### Measured (local cluster tests)

`bun run test:cluster` in `packages/s3qlite` runs real runner and client processes against
local Postgres and RustFS containers (`shardLockExpiration` 6 s, refresh 2 s, `pushDelayMs`
50 ms, one write every 50 ms per entity). Ranges over three runs (effect 4.0.5 twice, 4.0.1
once):

| Scenario | Durable replies | Fast replies | Notes |
| --- | --- | --- | --- |
| Graceful handoff (SIGTERM) | 0 lost (202–250 acked) | 0 lost (400–537 acked) | final push always before the next activation; max gap between acks 2.5–6.8 s |
| Hung runner (SIGSTOP, SIGCONT after takeover) | 0 lost, 0 acks from the stale owner | 1–3 lost | takeover 7.3–9.8 s; the resumed owner's writes are fenced |
| Crash (SIGKILL) | 0 lost | 1–5 lost, all acknowledged ≤ 185 ms before the kill | takeover 6.9–7.9 s |
| Runner joins (rebalance) | 0 lost | 0 lost | ordered handoffs; max ack gap 5.1–5.8 s |
| In-flight RPCs + retries (volatile and persisted) | 0 lost, 0 duplicates | | 66–104 writes retried while the first attempt was still running |
| Reactivation cost (about 50 rows, 50 batches) | | | create 79–87 ms, local copy reused 9–23 ms, download 578–651 ms |
| Postgres head store | stale write fenced | | bucket mirror readable by non-owners |

Most of the unavailability comes from the cluster (lock expiry, assignment polling), not from
S3. Durable writes took about 50 ms (p50) against local RustFS; fast replies acknowledged about
twice as many writes in the same time.

## Explicit S3 Config

S3Qlite uses default AWS SDK configuration (env variables), but you can also provide explicit S3 configuration:

```ts
const db = await connect("app-123", {
	bucket: "s3qlite",
	localDirectory: "./.s3qlite",
	s3: {
		region: "us-east-1",
		credentials: {
			accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
			secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
		},
	},
});
```

# Operations

## `.sync(): Promise<void>`

Pulls changes from remote and applies them to the local database. Then it uploads local changes to the remote. This operation tries to resolve any issues that might arise during pull or push operations. Prefer using sync instead of pull and push separately, unless you have specific reasons to do so.

## `.pull(): Promise<void>`

Pulls changes from remote and applies them to the local database without pushing local changes back. If you have unpushed local changes, pull does a rollback-and-replay: it temporarily rolls back to your last synced state, applies the remote changes, then replays your local changes on top. This is atomic - your database stays untouched if anything goes wrong.

## `.push(): Promise<void>`

Uploads local changes to the remote. If the remote has any changes that are not present locally (the local database is not pulled to the latest remote state), this operation will fail - this might also happen when other instances push their changes between your pull & push operations - use sync to not worry about this.

Push fails with typed errors in the Effect API:

- `ConflictError` when the remote changed during push and the CAS update is rejected.

## `.fork(name: string): Promise<void>`

Creates a new database name that starts from the current remote of the source database. This copies only the metadata, so the fork reuses the same referenced snapshot and batch objects already stored in S3. After the fork, both databases can diverge independently.

Fork fails with typed errors in the Effect API:

- `SameNameError` when the target name matches the source database name.
- `AlreadyExistsError` when the target database already exists.
- `SourceDoesNotExistError` when the source database does not exist remotely.

## `from(pathToFile, dbName, options): Promise<void>`

A standalone function that imports an existing local SQLite database into S3Qlite.

```ts
import { connect, from } from "@lika85456/s3qlite";

await from("path/to/local.db", "imported-db", {
	bucket: "s3qlite",
});

const db = await connect("imported-db", {
	bucket: "s3qlite",
});
```

Typed errors in the Effect API:

- `AlreadyExistsError` when a database with the target name already exists.
- `FileNotFoundError` when the source file does not exist.

# Crash safety

- Owner mode: a crash at any point of a push (batch upload, head compare-and-set, local head
  write, CDC prune) leaves a gap-free history. A compare-and-set whose response was lost is
  recognized on the next push (the head is the one it wrote) instead of fencing the owner. A
  restart in the same local directory pushes writes that were committed but not pushed.

- Creating a database uploads the snapshot first and then creates the head with
  `If-None-Match: *`. A published head never points to a missing snapshot, and when two
  clients create the same database at once the loser adopts the winner's head.
- Pushes publish the head with `If-Match` (compare-and-swap on the head ETag).
- A pull builds the new database next to the live one. It is committed by writing a pending
  head; then the live database is replaced and the head adopted with atomic renames. A failure
  before the commit leaves the old database and connection untouched. A failure after it is
  rolled forward immediately, or on the next `connect` after a crash.
- The local file store serializes compare-and-swap writes with a lock file and creates files
  with an exclusive `link(2)`, so concurrent processes cannot both win.

# How does it work?

S3Qlite uses Turso CDC to store locally created changes. During sync these changes are batched and synced to/from the provided S3 bucket and applied, so multiple instances can do work at the same time on the same database with minimal S3 requests and minimal transfered data. Thanks to this approach a point in-time state of the database can be reconstructed as well as easily forking the database without additional overhead.

# Limitations

- Due to the last push wins strategy, a unwanted "corruption" of data might occur. Have in mind that the synchronization is just application of CDC rows - if two instances update the same row, the last pushed edit wins. Try to avoid autoincrement and prefer other databases for more transactional workloads. If you want the most eager built-in syncing, use `syncMode: { type: "transaction" }`.
- Snapshots (checkpoints) exist only in owner mode. Other modes keep every batch in the head.
- Turso aborts the process (a Rust panic) on `ALTER TABLE ... DROP COLUMN` inside a transaction
  while an older prepared statement is still open. S3Qlite closes every statement it prepares;
  close your own statements when you use the `Database` API directly.
- Pull has to close and reopen the local connection. Rows from `iterate()` stream outside the
  sync queue, so finish an iteration before a pull runs.
