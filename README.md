# MySQL Binlog Replicator

Standalone Spring Boot app that snapshots a MySQL source database and then applies MySQL binlog changes to a MySQL sink database without Kafka or Debezium.

The app uses `com.zendesk:mysql-binlog-connector-java:0.30.1`, a maintained fork of the original Shyiko connector. This is required for MySQL 8/9 SHA2 authentication support.

## Documentation

- [Replication flow and operating docs](docs/replication-flow.md)

## Requirements

Source MySQL must have:

```sql
log_bin = ON
binlog_format = ROW
binlog_row_image = FULL
server_id = unique
```

For an existing MySQL source, create a dedicated user:

```sql
CREATE USER 'replicator'@'%' IDENTIFIED BY 'replicator';
GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
FLUSH PRIVILEGES;
```

GTID is used when enabled and a checkpoint has `gtid_set`; otherwise the app resumes from `binlog_file` and `binlog_position`.

## Run

```bash
docker compose up --build
```

If you already created the local MySQL containers before this auth change, recreate the source container and its volume so `/docker-entrypoint-initdb.d` runs again:

```bash
docker compose down -v
docker compose up --build
```

Health:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/replication/status
```

Pause/resume:

```bash
curl -X POST http://localhost:8080/replication/pause
curl -X POST http://localhost:8080/replication/resume
```

## Configuration

Minimal replication config:

```yaml
source:
  url: jdbc:mysql://localhost:3309/thinkvitals?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
  username: root
  password: password

sink:
  url: jdbc:mysql://localhost:3310/thinkvitals_bcp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
  username: root
  password: password

replication:
  serverId: 987654
  useGtid: true
  snapshotMode: initial
  ddlEnabled: true
  strictDdl: false
  dmlEnabled: true
  includeTables: []
  excludeTables:
    - flyway_schema_history
    - replication_checkpoint
  onDuplicateInsert: upsert
  requirePrimaryKey: true
```

### Relaxed DDL Mode

`replication.strictDdl` defaults to `false`.

With `strictDdl: false`, sink DDL is intentionally relaxed so schema creation is easier during snapshot and CDC:

- `CREATE TABLE` keeps writable source columns and `PRIMARY KEY`.
- Non-primary-key text columns such as `varchar`, `char`, `text`, `enum`, and `set` are widened to `longtext`; binary/blob columns are widened to `longblob`.
- Per-column defaults, `ON UPDATE`, `AUTO_INCREMENT`, `NOT NULL`, generated columns, per-column charset/collation, and comments are removed.
- Foreign keys, unique constraints, secondary indexes, fulltext/spatial indexes, and check constraints are removed from sink `CREATE TABLE`.
- Table options such as `ENGINE`, `AUTO_INCREMENT`, default charset, and default collation are removed so the sink database defaults are used.
- `CREATE INDEX`, `DROP INDEX`, and index/constraint-only `ALTER TABLE` events are skipped and checkpointed.
- Column-changing DDL such as `ALTER TABLE ... ADD COLUMN`, `MODIFY COLUMN`, `CHANGE COLUMN`, `DROP COLUMN`, and `RENAME COLUMN` is still applied.

This mode is useful when the sink is a replication target where FK ordering and extra constraints should not block data sync. DML consistency still relies on primary keys: `UPDATE` and `DELETE` require primary keys by default, and `INSERT` uses upsert mode by default.

If a sink table was created before relaxed widening existed, DML data truncation errors for string/blob columns are handled by widening the affected sink column and retrying the event inside the same sink transaction.

Set `strictDdl: true` if the sink must preserve source constraints and indexes exactly. In strict mode, DDL failures stop the pipeline and the checkpoint is not advanced.

## Consistency Notes

Initial snapshot first tries `FLUSH TABLES WITH READ LOCK`, captures binary log status and GTID state, creates sink schema, copies rows, then stores the checkpoint. On MySQL versions that support it the app uses `SHOW BINARY LOG STATUS`; older versions fall back to `SHOW MASTER STATUS`. If the source user cannot run the lock, the app logs a warning and proceeds, but writes during the copy can make the snapshot inconsistent.

Checkpoint is stored in the sink table `replication_checkpoint` and is updated only after the sink DDL or sink transaction commits.

Per-table sync metadata is stored in the sink table `replication_table_sync_metadata`.

Important columns:

- `source_database`, `sink_database`, `table_name`: table identity.
- `snapshot_status`: `RUNNING`, `COMPLETED`, or `FAILED`.
- `snapshot_started_at`, `snapshot_completed_at`, `snapshot_rows_copied`: initial snapshot progress per table.
- `last_event_type`, `last_binlog_file`, `last_binlog_position`, `last_gtid_set`: last applied CDC/DDL event for that table.
- `last_event_time`, `last_applied_time`: source event time and sink apply time.
- `apply_count`: number of successful table-level CDC/DDL apply records.
- `last_error`: last snapshot/apply error captured for that table.

This table is metadata only. Resume still uses `replication_checkpoint` as the single ordered checkpoint so binlog ordering stays global.

## Current Defaults

`UPDATE` and `DELETE` require primary keys by default. Tables without primary keys are rejected for those operations to avoid ambiguous writes. `INSERT` defaults to `ON DUPLICATE KEY UPDATE` and can be changed with `replication.onDuplicateInsert=strict`. Sink DDL defaults to relaxed mode with `replication.strictDdl=false`.
