package com.example.replicator;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class CheckpointService {
    private static final Logger log = LoggerFactory.getLogger(CheckpointService.class);

    private final JdbcTemplate sink;
    private final ReplicatorProperties properties;
    private final JdbcUrlParser.MysqlEndpoint sourceEndpoint;
    private final JdbcUrlParser.MysqlEndpoint sinkEndpoint;
    private final AtomicReference<ReplicationStatus> status = new AtomicReference<>();

    public CheckpointService(@Qualifier("sinkJdbcTemplate") JdbcTemplate sink, ReplicatorProperties properties) {
        this.sink = sink;
        this.properties = properties;
        this.sourceEndpoint = JdbcUrlParser.parse(properties.getSource().getUrl(), properties.getSource().getDatabase());
        this.sinkEndpoint = JdbcUrlParser.parse(properties.getSink().getUrl(), properties.getSink().getDatabase());
        ensureTable();
        status.set(new ReplicationStatus(false, false, sourceEndpoint.database(), sinkEndpoint.database(),
                null, null, null, null, null, null, null, null, false, null, null));
    }

    void ensureTable() {
        sink.execute("""
                CREATE TABLE IF NOT EXISTS replication_checkpoint (
                    id INT PRIMARY KEY,
                    source_database VARCHAR(255) NOT NULL,
                    binlog_file VARCHAR(255) NULL,
                    binlog_position BIGINT NULL,
                    gtid_set TEXT NULL,
                    last_event_type VARCHAR(50) NULL,
                    last_table_name VARCHAR(255) NULL,
                    last_event_time DATETIME(3) NULL,
                    last_applied_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
                )
                """);
        sink.execute("""
                CREATE TABLE IF NOT EXISTS replication_table_sync_metadata (
                    source_database VARCHAR(255) NOT NULL,
                    sink_database VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    snapshot_status VARCHAR(50) NULL,
                    snapshot_started_at DATETIME(3) NULL,
                    snapshot_completed_at DATETIME(3) NULL,
                    snapshot_rows_copied BIGINT NOT NULL DEFAULT 0,
                    last_event_type VARCHAR(50) NULL,
                    last_binlog_file VARCHAR(255) NULL,
                    last_binlog_position BIGINT NULL,
                    last_gtid_set TEXT NULL,
                    last_event_time DATETIME(3) NULL,
                    last_applied_time DATETIME(3) NULL,
                    apply_count BIGINT NOT NULL DEFAULT 0,
                    last_error TEXT NULL,
                    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                    PRIMARY KEY (source_database, table_name)
                )
                """);
    }

    Optional<BinlogPosition> load() {
        List<BinlogPosition> rows = sink.query("""
                        SELECT source_database, binlog_file, binlog_position, gtid_set,
                               last_event_type, last_table_name, last_event_time
                        FROM replication_checkpoint WHERE id = 1
                        """,
                (rs, rowNum) -> new BinlogPosition(
                        rs.getString("binlog_file"),
                        rs.getObject("binlog_position", Long.class),
                        rs.getString("gtid_set"),
                        rs.getString("last_event_type"),
                        rs.getString("last_table_name"),
                        rs.getTimestamp("last_event_time") == null ? null : rs.getTimestamp("last_event_time").toLocalDateTime(),
                        rs.getString("source_database")
                ));
        Optional<BinlogPosition> checkpoint = rows.stream().findFirst();
        checkpoint.ifPresent(position -> {
            if (position.sourceDatabase() != null
                    && !position.sourceDatabase().equalsIgnoreCase(sourceEndpoint.database())) {
                log.warn("Checkpoint source_database={} differs from configured source database={}; resuming from id=1 checkpoint because this app has a single checkpoint stream",
                        position.sourceDatabase(), sourceEndpoint.database());
            }
        });
        return checkpoint;
    }

    void save(BinlogPosition position) {
        sink.update("""
                        INSERT INTO replication_checkpoint (
                            id, source_database, binlog_file, binlog_position, gtid_set,
                            last_event_type, last_table_name, last_event_time, last_applied_time
                        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
                        ON DUPLICATE KEY UPDATE
                            source_database = VALUES(source_database),
                            binlog_file = VALUES(binlog_file),
                            binlog_position = VALUES(binlog_position),
                            gtid_set = VALUES(gtid_set),
                            last_event_type = VALUES(last_event_type),
                            last_table_name = VALUES(last_table_name),
                            last_event_time = VALUES(last_event_time),
                            last_applied_time = VALUES(last_applied_time)
                        """,
                sourceEndpoint.database(),
                position.binlogFile(),
                position.binlogPosition(),
                position.gtidSet(),
                position.lastEventType(),
                position.lastTableName(),
                position.lastEventTime() == null ? null : Timestamp.valueOf(position.lastEventTime()));
        refreshStatus(true, false, null);
    }

    void saveWithConnection(Connection connection, BinlogPosition position) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO replication_checkpoint (
                    id, source_database, binlog_file, binlog_position, gtid_set,
                    last_event_type, last_table_name, last_event_time, last_applied_time
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
                ON DUPLICATE KEY UPDATE
                    source_database = VALUES(source_database),
                    binlog_file = VALUES(binlog_file),
                    binlog_position = VALUES(binlog_position),
                    gtid_set = VALUES(gtid_set),
                    last_event_type = VALUES(last_event_type),
                    last_table_name = VALUES(last_table_name),
                    last_event_time = VALUES(last_event_time),
                    last_applied_time = VALUES(last_applied_time)
                """)) {
            ps.setString(1, sourceEndpoint.database());
            ps.setString(2, position.binlogFile());
            if (position.binlogPosition() == null) {
                ps.setObject(3, null);
            } else {
                ps.setLong(3, position.binlogPosition());
            }
            ps.setString(4, position.gtidSet());
            ps.setString(5, position.lastEventType());
            ps.setString(6, position.lastTableName());
            ps.setTimestamp(7, position.lastEventTime() == null ? null : Timestamp.valueOf(position.lastEventTime()));
            ps.executeUpdate();
        }
    }

    void markSnapshotStarted(String table) {
        sink.update("""
                        INSERT INTO replication_table_sync_metadata (
                            source_database, sink_database, table_name, snapshot_status,
                            snapshot_started_at, snapshot_rows_copied, last_error
                        ) VALUES (?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP(3), 0, NULL)
                        ON DUPLICATE KEY UPDATE
                            sink_database = VALUES(sink_database),
                            snapshot_status = VALUES(snapshot_status),
                            snapshot_started_at = VALUES(snapshot_started_at),
                            snapshot_rows_copied = VALUES(snapshot_rows_copied),
                            last_error = VALUES(last_error)
                        """,
                sourceEndpoint.database(), sinkEndpoint.database(), table);
    }

    void markSnapshotCompleted(String table, long rowsCopied) {
        sink.update("""
                        INSERT INTO replication_table_sync_metadata (
                            source_database, sink_database, table_name, snapshot_status,
                            snapshot_completed_at, snapshot_rows_copied, last_applied_time, last_error
                        ) VALUES (?, ?, ?, 'COMPLETED', CURRENT_TIMESTAMP(3), ?, CURRENT_TIMESTAMP(3), NULL)
                        ON DUPLICATE KEY UPDATE
                            sink_database = VALUES(sink_database),
                            snapshot_status = VALUES(snapshot_status),
                            snapshot_completed_at = VALUES(snapshot_completed_at),
                            snapshot_rows_copied = VALUES(snapshot_rows_copied),
                            last_applied_time = VALUES(last_applied_time),
                            last_error = VALUES(last_error)
                        """,
                sourceEndpoint.database(), sinkEndpoint.database(), table, rowsCopied);
    }

    void markTableError(String table, String error) {
        sink.update("""
                        INSERT INTO replication_table_sync_metadata (
                            source_database, sink_database, table_name, snapshot_status, last_error
                        ) VALUES (?, ?, ?, 'FAILED', ?)
                        ON DUPLICATE KEY UPDATE
                            sink_database = VALUES(sink_database),
                            snapshot_status = VALUES(snapshot_status),
                            last_error = VALUES(last_error)
                        """,
                sourceEndpoint.database(), sinkEndpoint.database(), table, error);
    }

    void saveTableEventsWithConnection(Connection connection, Collection<String> tables, BinlogPosition position) throws Exception {
        Set<String> uniqueTables = new LinkedHashSet<>(tables);
        uniqueTables.removeIf(table -> table == null || table.isBlank());
        if (uniqueTables.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO replication_table_sync_metadata (
                    source_database, sink_database, table_name, last_event_type,
                    last_binlog_file, last_binlog_position, last_gtid_set, last_event_time,
                    last_applied_time, apply_count, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3), 1, NULL)
                ON DUPLICATE KEY UPDATE
                    sink_database = VALUES(sink_database),
                    last_event_type = VALUES(last_event_type),
                    last_binlog_file = VALUES(last_binlog_file),
                    last_binlog_position = VALUES(last_binlog_position),
                    last_gtid_set = VALUES(last_gtid_set),
                    last_event_time = VALUES(last_event_time),
                    last_applied_time = VALUES(last_applied_time),
                    apply_count = apply_count + 1,
                    last_error = VALUES(last_error)
                """)) {
            for (String table : uniqueTables) {
                ps.setString(1, sourceEndpoint.database());
                ps.setString(2, sinkEndpoint.database());
                ps.setString(3, table);
                ps.setString(4, position.lastEventType());
                ps.setString(5, position.binlogFile());
                if (position.binlogPosition() == null) {
                    ps.setObject(6, null);
                } else {
                    ps.setLong(6, position.binlogPosition());
                }
                ps.setString(7, position.gtidSet());
                ps.setTimestamp(8, position.lastEventTime() == null ? null : Timestamp.valueOf(position.lastEventTime()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    void saveTableEvents(Collection<String> tables, BinlogPosition position) {
        Set<String> uniqueTables = new LinkedHashSet<>(tables);
        uniqueTables.removeIf(table -> table == null || table.isBlank());
        for (String table : uniqueTables) {
            sink.update("""
                            INSERT INTO replication_table_sync_metadata (
                                source_database, sink_database, table_name, last_event_type,
                                last_binlog_file, last_binlog_position, last_gtid_set, last_event_time,
                                last_applied_time, apply_count, last_error
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3), 1, NULL)
                            ON DUPLICATE KEY UPDATE
                                sink_database = VALUES(sink_database),
                                last_event_type = VALUES(last_event_type),
                                last_binlog_file = VALUES(last_binlog_file),
                                last_binlog_position = VALUES(last_binlog_position),
                                last_gtid_set = VALUES(last_gtid_set),
                                last_event_time = VALUES(last_event_time),
                                last_applied_time = VALUES(last_applied_time),
                                apply_count = apply_count + 1,
                                last_error = VALUES(last_error)
                            """,
                    sourceEndpoint.database(),
                    sinkEndpoint.database(),
                    table,
                    position.lastEventType(),
                    position.binlogFile(),
                    position.binlogPosition(),
                    position.gtidSet(),
                    position.lastEventTime() == null ? null : Timestamp.valueOf(position.lastEventTime()));
        }
    }

    void refreshStatus(boolean running, boolean paused, String error) {
        Optional<BinlogPosition> cp = load();
        LocalDateTime applied = sink.query("""
                        SELECT last_applied_time FROM replication_checkpoint WHERE id = 1
                        """,
                rs -> rs.next() ? rs.getTimestamp("last_applied_time").toLocalDateTime() : null);
        BinlogPosition p = cp.orElse(new BinlogPosition(null, null, null, null, null, null, null));
        Long lagMs = p.lastEventTime() == null ? null : Instant.now().toEpochMilli()
                - p.lastEventTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        status.set(new ReplicationStatus(running, paused, sourceEndpoint.database(), sinkEndpoint.database(),
                p.binlogFile(), p.binlogPosition(), p.gtidSet(), p.lastTableName(), p.lastEventType(),
                p.lastEventTime(), applied, lagMs, cp.isPresent(), p.sourceDatabase(), error));
    }

    ReplicationStatus status() {
        return status.get();
    }

    JdbcUrlParser.MysqlEndpoint sourceEndpoint() {
        return sourceEndpoint;
    }

    JdbcUrlParser.MysqlEndpoint sinkEndpoint() {
        return sinkEndpoint;
    }

    ReplicatorProperties properties() {
        return properties;
    }
}
