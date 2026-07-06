# Project overview

This package is a simple wrapper over the tursodatabase. What it does is it enables clients to sync the database into s3 compatible remote storage and it also provides API to handle pulling, pushing or forking the database.

It is not yet fully implemented and the code is currently very experimental.

# Data primitives

## Snapshot

A snapshot is a fully working database file. It is a single file, so upon creation from a live database the TRUNCATE operation must be called to truncate the WAL changes. If a snapshot was created from a checkpoint operation, then the metadata of the snapshot will contain the previous snapshot and the batches applied to it.

## Batch

Batch is a collection of changes, currently in the form of CDC list. For transport and storage a protobuf is used. Batches have metadata containing the `fromId` and `toId` which represents the ids of the CDC changes included in the batch (inclusive).

## Head

A head is a pointer to a database state, it is represented by a snapshot and a list of batches.

# Operations

Except normal SQL operations and what the @tursodatabase/database provides, this library wraps the Database class and provides additional operations.

- pull: It pulls the new batches from remote and tries to apply them on a rollbacked version of the clients local database. Only after the new batches are applied successfully, the local changes are replayed on top and the local database is switched to this new state. This operation is atomic and if one of the steps fails, the local database is not changed.

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

The local directory (by default `./.s3qlite/`) is expected to have the following structure:

- `{dbName}.json` - last synced remote head file
- `{uuid}.batch` - batch file
- `{uuid}.snapshot` - snapshot file
- `{dbName}.db|.wal|.shm` - database files created by tursodatabase.

# Known issues

- Colliding primary-key changes currently resolve with a last-push-wins strategy during sync. If a different merge strategy is needed, add an explicit conflict-resolution option instead of changing the default implicitly.