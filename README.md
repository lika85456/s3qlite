<img src="./apps/docs/public/s3qlite_logo.png" width="400" alt="S3Qlite" />

Fast in-process, local first SQLite database powered by [Turso](https://turso.tech/) synced to S3 compatible storage for durability and distribution. In other words: self hosted Turso Cloud with any S3 compatible storage provider.

# Installation

`bun add @lika85456/s3qlite`

# Usage

S3Qlite extends Turso database API with additional methods for synchronization. These operations were inspired by the [Turso Sync](https://docs.turso.tech/sync/usage)

## Promise API

```ts
import { connect } from "@lika85456/s3qlite";

const db = await connect("agent-123", {
	bucket: "s3qlite", // single bucket can store multiple databases
	localDirectory: "./.s3qlite", // the local database, batches and other files need to be stored in localDirectory
});

await db.exec(`
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL
	)
`);

await db.run("INSERT INTO users (id, name) VALUES (?, ?)", "1", "alice");
await db.sync();

const users = await db.all("SELECT * FROM users ORDER BY id");
console.log(users);

await db.close();
```

This relies on the default AWS SDK configuration resolution, such as `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.

## Effect API

```ts
import { connect } from "@lika85456/s3qlite/effect";
import { Effect } from "effect";

const program = Effect.gen(function* () {
	const db = yield* connect("app.db", {
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

	yield* Effect.tryPromise(() => db.close());
});

await Effect.runPromise(program);
```

## Explicit S3 Config

```ts
const db = await connect("app.db", {
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

## Sync

Pulls changes from remote and applies them to the local database. Then it uploads local changes to the remote. This operation tries to resolve any issues that might arise during pull or push operations. Prefer using sync instead of pull and push separately, unless you have specific reasons to do so.

## Pull

Pulls changes from remote and applies them to the local database without pushing local changes back. If you have unpushed local changes, pull does a rollback-and-replay: it temporarily rolls back to your last synced state, applies the remote changes, then replays your local changes on top. This is atomic — your database stays untouched if anything goes wrong.

## Push

Uploads local changes to the remote. If the remote has any changes that are not present locally (the local database is not pulled to the latest remote state), this operation will fail - this might also happen when other instances push their changes between your pull & push operations - use sync to not worry about this.

# How does it work?

S3Qlite uses Turso CDC to store changes in the local database. During sync these changes are batched and synced to/from S3 bucket and applied. Thanks to this approach, from snapshot files and batch files a point in-time state of the database can be reconstructed as well as easily forking the database without much overhead.

# Limitations

- Due to the last push wins strategy, a unwanted "corruption" of data might occur. Have in mind that the synchronization is just smart merging of CDC rows. Try to avoid autoincrement and prefer other databases for more transactional workloads. S3Qlite does support fully transactional model (basically calls sync before and after each query)
- Currently fork or checkpointing is not implemented, it is planned for future.
