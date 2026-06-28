# S3Qlite

Fast in-process, local first SQLite database powered by [Turso](https://turso.tech/) synced to S3 compatible storage for durability and distribution. In other words: self hosted Turso Cloud with any S3 compatible storage provider.

# Installation

`bun add @lika85456/s3qlite`

# Usage

S3Qlite re-exports the Turso database API with additional methods for synchronization. These operations were inspired by the [Turso Sync](https://docs.turso.tech/sync/usage)

## Sync

Pulls changes from remote and applies them to the local database. Then it uploads local changes to the remote. This operation tries to resolve any issues that might arise during pull or push operations. Prefer using sync instead of pull and push separately, unless you have specific reasons to do so.

## Pull

Pulls changes from remote and applies them to the local database. It does not push any changes to remote. This operation can fail if the local changes are not compatible with the remote changes.

## Push

Uploads local changes to the remote. It requires the local database to be in the latest remote state, as merging divergent database states is not supported yet. If the remote has any changes that are not present locally, this operation will fail - this might also happen when other instances push their changes between your pull & push operations.
