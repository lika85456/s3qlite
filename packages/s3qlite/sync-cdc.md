# Turso Sync CDC

This is the CDC path used by Turso Sync for push-style replication.

## Big picture

- Local changes are captured through `capture_data_changes_conn` into a CDC table.
- Sync reads that CDC table, converts each record into a replayable row change, then turns those changes into SQL statements.
- The remote receives a transactional SQL batch, not raw CDC rows.

## How CDC is enabled

- Every tracked connection runs:
    - `PRAGMA capture_data_changes_conn('full,turso_cdc')`
- Defaults:
    - CDC table: `turso_cdc`
    - CDC mode: `full`
- The sync wrapper caches the CDC version from `turso_cdc_version`.

Source flow:

- `DatabaseTape::connect()` sets the pragma for each new connection.
- `DatabaseTape::iterate_changes()` later reads from the CDC table.

## CDC table shape

The sync engine expects CDC rows in these layouts:

### V1

8 columns:

1. `change_id`
2. `change_time`
3. `change_type`
4. `table_name`
5. `id`
6. `before`
7. `after`
8. `updates`

### V2

9 columns:

1. `change_id`
2. `change_time`
3. `change_txn_id`
4. `change_type`
5. `table_name`
6. `id`
7. `before`
8. `after`
9. `updates`

Change types:

- `-1` = delete
- `0` = update
- `1` = insert
- `2` = commit

## How CDC is retrieved

The retrieval path is:

1. Open a fresh tracked connection.
2. Query the CDC table with `SELECT * FROM turso_cdc WHERE change_id ... ORDER BY change_id ... LIMIT ...`.
3. Convert each row into `DatabaseChange`.
4. Convert each `DatabaseChange` into a replayable `DatabaseTapeOperation`.

Important detail:

- `Commit` records are emitted as explicit `DatabaseTapeOperation::Commit` markers.
- Those markers are used to preserve transaction boundaries.

## Internal change representation

The sync layer normalizes CDC into:

- `DatabaseChange`
    - raw CDC row
- `DatabaseTapeRowChange`
    - one row-level change with `change_id`, `change_time`, `table_name`, `id`, and typed payload
- `DatabaseTapeOperation`
    - `RowChange(...)`
    - `Commit`

For data payloads:

- insert: `after`
- delete: `before`
- update: `before`, `after`, optionally `updates`

The `updates` blob for updates is interpreted as:

- first half: bitset flags per column
- second half: actual updated values

## Exact application rules

The replay layer builds SQL from CDC-derived row changes.

### DML

#### Insert

- Build an `INSERT INTO ... VALUES ...` statement.
- If the table has a primary key, use `ON CONFLICT(pk) DO UPDATE SET ...` for upsert-style replay.
- If `use_implicit_rowid` is enabled, append `rowid`.

#### Delete

- If a primary key exists and `use_implicit_rowid` is disabled:
    - generate `DELETE FROM table WHERE pk1 = ? AND pk2 = ? ...`
- Otherwise:
    - generate `DELETE FROM table WHERE rowid = ?`

#### Update

- If CDC `updates` is present:
    - only changed columns are used to build the SQL values.
- If `updates` is absent:
    - replay as a full-row update based on the `after` image.
- The generated SQL is:
    - `UPDATE table SET col1 = ?, ... WHERE pk = ?`

## DDL handling

DDL is tracked as CDC on `sqlite_schema`.

### Delete from `sqlite_schema`

- Interpreted as drop DDL.
- The engine reads `type` and `name` from the CDC `before` image.
- It generates:
    - `DROP <type> <name>`

### Insert into `sqlite_schema`

- Interpreted as create DDL.
- The engine reads the stored SQL from the last column of the CDC `after` image.
- For these statement types it forces idempotency by adding `IF NOT EXISTS` when possible:
    - `CREATE TABLE`
    - `CREATE INDEX`
    - `CREATE TRIGGER`
    - `CREATE VIEW`
    - `CREATE MATERIALIZED VIEW`
- The resulting SQL is then replayed directly.

### Update to `sqlite_schema`

- Interpreted as altered DDL.
- The engine reads the new SQL text from the CDC `updates` blob.
- This is mainly how `ALTER TABLE ... ADD COLUMN` is replayed.

## Idempotency rules

The replay layer is conservative and only special-cases a small set of schema-extending changes.

### `CREATE ...`

- Replay adds `IF NOT EXISTS` where supported.

### `ALTER TABLE ... ADD COLUMN`

- The replay generator checks `pragma_table_info(table)`.
- If the column already exists, the statement is skipped.
- Otherwise it is executed.

### Other DDL

- If the change is not one of the above, it is executed directly.
- There is no general-purpose DDL merge engine here.

## Transaction boundaries

CDC replay respects commit markers.

- Row changes are buffered within a transaction.
- On `Commit`:
    - the replay session issues `COMMIT`
    - the transaction state resets
- On the next row change:
    - replay starts a new `BEGIN IMMEDIATE`

So the engine preserves the source transaction grouping rather than applying each row independently.

## What gets sent to the remote

CDC is not sent raw.

The sync engine converts CDC changes into a batch of SQL statements:

- `BEGIN IMMEDIATE`
- DDL bootstrap statement for sync bookkeeping
- one SQL statement per CDC-derived change
- update of `turso_sync_last_change_id`
- `COMMIT`

That batch is sent over the Hrana `/v2/pipeline` protocol.

## Practical implementation recipe

If you want to reimplement the same idea:

1. Enable CDC on every write connection.
2. Persist CDC rows in a stable table with monotonic `change_id`.
3. Emit explicit commit markers.
4. Parse CDC into a normalized row-change structure.
5. Special-case `sqlite_schema` for DDL.
6. Generate SQL replay from row changes.
7. Batch replay in a single transaction.
8. Make schema-extending DDL idempotent.
9. Store a last-synced change cursor per client.

## Important constraints

- This design assumes CDC rows are complete enough to reconstruct the original row mutation.
- DDL support is partial, not a full schema conflict resolver.
- `ALTER TABLE ADD COLUMN` is the main hard case they explicitly handle.
- Pull-sync is separate and page-based; this document only covers CDC push.