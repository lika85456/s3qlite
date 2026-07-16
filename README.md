<div align="center">
	<img src="./apps/docs/public/s3qlite_logo.png" width="400" alt="S3Qlite" />
</div>

In-process SQL database with sub-millisecond latency persisted on S3 for durability and easy distribution.

This library provides a wrapper over [Turso database](https://turso.tech/) extending it with additional methods for synchronization.

Tired of not paying a single cent with the generous free tier of Turso Cloud? Run your own!

# Installation

`bun add @lika85456/s3qlite`

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
- `{ type: "transaction" }`: pulls before every wrapped database operation, then pushes after it if the local CDC change id increased.
- `{ type: "push-on-change" }`: never pulls automatically, but pushes after a wrapped operation if the local CDC change id increased.

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

## .sync(): Promise<void>

Pulls changes from remote and applies them to the local database. Then it uploads local changes to the remote. This operation tries to resolve any issues that might arise during pull or push operations. Prefer using sync instead of pull and push separately, unless you have specific reasons to do so.

## .pull(): Promise<void>

Pulls changes from remote and applies them to the local database without pushing local changes back. If you have unpushed local changes, pull does a rollback-and-replay: it temporarily rolls back to your last synced state, applies the remote changes, then replays your local changes on top. This is atomic - your database stays untouched if anything goes wrong.

## .push(): Promise<void>

Uploads local changes to the remote. If the remote has any changes that are not present locally (the local database is not pulled to the latest remote state), this operation will fail - this might also happen when other instances push their changes between your pull & push operations - use sync to not worry about this.

Push fails with typed errors in the Effect API:

- `ConflictError` when the remote changed during push and the CAS update is rejected.

## .fork(name: string): Promise<void>

Creates a new database name that starts from the current remote of the source database. This copies only the metadata, so the fork reuses the same referenced snapshot and batch objects already stored in S3. After the fork, both databases can diverge independently.

Fork fails with typed errors in the Effect API:

- `SameNameError` when the target name matches the source database name.
- `AlreadyExistsError` when the target database already exists.
- `SourceDoesNotExistError` when the source database does not exist remotely.

## from(pathToFile, dbName, options): Promise<void>

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

# How does it work?

S3Qlite uses Turso CDC to store locally created changes. During sync these changes are batched and synced to/from the provided S3 bucket and applied, so multiple instances can do work at the same time on the same database with minimal S3 requests and minimal transfered data. Thanks to this approach a point in-time state of the database can be reconstructed as well as easily forking the database without additional overhead.

# Limitations

- Due to the last push wins strategy, a unwanted "corruption" of data might occur. Have in mind that the synchronization is just application of CDC rows - if two instances update the same row, the last pushed edit wins. Try to avoid autoincrement and prefer other databases for more transactional workloads. If you want the most eager built-in syncing, use `syncMode: { type: "transaction" }`.
- Checkpointing is not implemented yet.
