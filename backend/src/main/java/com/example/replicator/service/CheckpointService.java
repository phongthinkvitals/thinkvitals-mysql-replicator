package com.example.replicator.service;

import com.example.replicator.config.ReplicatorProperties;
import com.example.replicator.model.BinlogPosition;
import com.example.replicator.model.ReplicationStatus;
import com.example.replicator.util.JdbcUrlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
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

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    // The app uses a single fixed checkpoint row to resume binlog replication.
    private static final int SINGLE_CHECKPOINT_ID = 1;

    private final JdbcTemplate sinkJdbcTemplate;
    private final ReplicatorProperties replicatorProperties;
    private final JdbcUrlParser.MysqlEndpoint sourceMysqlEndpoint;
    private final JdbcUrlParser.MysqlEndpoint sinkMysqlEndpoint;

    // Holds the latest replication status for fast status/API reads.
    private final AtomicReference<ReplicationStatus> replicationStatusReference = new AtomicReference<>();

    public CheckpointService(
            @Qualifier("sinkJdbcTemplate") JdbcTemplate sinkJdbcTemplate,
            ReplicatorProperties replicatorProperties
    ) {
        this.sinkJdbcTemplate = sinkJdbcTemplate;
        this.replicatorProperties = replicatorProperties;

        this.sourceMysqlEndpoint = JdbcUrlParser.parse(
                replicatorProperties.getSource().getUrl(),
                replicatorProperties.getSource().getDatabase()
        );

        this.sinkMysqlEndpoint = JdbcUrlParser.parse(
                replicatorProperties.getSink().getUrl(),
                replicatorProperties.getSink().getDatabase()
        );

        ensureTable();

        // Initialize the default status before replication starts.
        replicationStatusReference.set(new ReplicationStatus(
                false,
                false,
                sourceMysqlEndpoint.database(),
                sinkMysqlEndpoint.database(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                null,
                null
        ));
    }

    private void ensureTable() {
        // Stores the last successfully applied binlog checkpoint.
        sinkJdbcTemplate.execute("""
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

        // Stores snapshot and CDC metadata for each replicated table.
        sinkJdbcTemplate.execute("""
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

    public Optional<BinlogPosition> load() {
        // Read the current checkpoint from the sink database.
        List<BinlogPosition> checkpointPositions = sinkJdbcTemplate.query("""
                        SELECT source_database, binlog_file, binlog_position, gtid_set,
                               last_event_type, last_table_name, last_event_time
                        FROM replication_checkpoint
                        WHERE id = ?
                        """,
                preparedStatement -> preparedStatement.setInt(1, SINGLE_CHECKPOINT_ID),
                (resultSet, rowNumber) -> {
                    Timestamp lastEventTimestamp = resultSet.getTimestamp("last_event_time");

                    return new BinlogPosition(
                            resultSet.getString("binlog_file"),
                            resultSet.getObject("binlog_position", Long.class),
                            resultSet.getString("gtid_set"),
                            resultSet.getString("last_event_type"),
                            resultSet.getString("last_table_name"),
                            lastEventTimestamp == null ? null : lastEventTimestamp.toLocalDateTime(),
                            resultSet.getString("source_database")
                    );
                }
        );

        Optional<BinlogPosition> checkpointPositionOptional = checkpointPositions.stream().findFirst();

        // Warn when the stored checkpoint belongs to a different source database.
        checkpointPositionOptional.ifPresent(loadedCheckpointPosition -> {
            String checkpointSourceDatabase = loadedCheckpointPosition.sourceDatabase();
            String configuredSourceDatabase = sourceMysqlEndpoint.database();

            if (checkpointSourceDatabase != null
                    && !checkpointSourceDatabase.equalsIgnoreCase(configuredSourceDatabase)) {
                logger.warn(
                        "Checkpoint source_database={} differs from configured source database={}; " +
                                "resuming from id={} checkpoint because this app has a single checkpoint stream",
                        checkpointSourceDatabase,
                        configuredSourceDatabase,
                        SINGLE_CHECKPOINT_ID
                );
            }
        });

        return checkpointPositionOptional;
    }

    public void save(BinlogPosition newCheckpointPosition) {
        // Upsert the checkpoint after an event has been applied successfully.
        sinkJdbcTemplate.update("""
                        INSERT INTO replication_checkpoint (
                            id, source_database, binlog_file, binlog_position, gtid_set,
                            last_event_type, last_table_name, last_event_time, last_applied_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
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
                SINGLE_CHECKPOINT_ID,
                sourceMysqlEndpoint.database(),
                newCheckpointPosition.binlogFile(),
                newCheckpointPosition.binlogPosition(),
                newCheckpointPosition.gtidSet(),
                newCheckpointPosition.lastEventType(),
                newCheckpointPosition.lastTableName(),
                newCheckpointPosition.lastEventTime() == null
                        ? null
                        : Timestamp.valueOf(newCheckpointPosition.lastEventTime())
        );

        refreshStatus(true, false, null);
    }

    public void saveWithConnection(Connection sinkConnection, BinlogPosition newCheckpointPosition) throws Exception {
        // Use the provided connection so checkpoint saving can share the same transaction.
        try (PreparedStatement checkpointUpsertStatement = sinkConnection.prepareStatement("""
                INSERT INTO replication_checkpoint (
                    id, source_database, binlog_file, binlog_position, gtid_set,
                    last_event_type, last_table_name, last_event_time, last_applied_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
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

            checkpointUpsertStatement.setInt(1, SINGLE_CHECKPOINT_ID);
            checkpointUpsertStatement.setString(2, sourceMysqlEndpoint.database());
            checkpointUpsertStatement.setString(3, newCheckpointPosition.binlogFile());

            if (newCheckpointPosition.binlogPosition() == null) {
                checkpointUpsertStatement.setObject(4, null);
            } else {
                checkpointUpsertStatement.setLong(4, newCheckpointPosition.binlogPosition());
            }

            checkpointUpsertStatement.setString(5, newCheckpointPosition.gtidSet());
            checkpointUpsertStatement.setString(6, newCheckpointPosition.lastEventType());
            checkpointUpsertStatement.setString(7, newCheckpointPosition.lastTableName());
            checkpointUpsertStatement.setTimestamp(
                    8,
                    newCheckpointPosition.lastEventTime() == null
                            ? null
                            : Timestamp.valueOf(newCheckpointPosition.lastEventTime())
            );

            checkpointUpsertStatement.executeUpdate();
        }
    }

    public void markSnapshotStarted(String tableName) {
        // Mark the table snapshot as started.
        sinkJdbcTemplate.update("""
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
                sourceMysqlEndpoint.database(),
                sinkMysqlEndpoint.database(),
                tableName
        );
    }

    public void markSnapshotCompleted(String tableName, long copiedRowCount) {
        // Mark the table snapshot as completed.
        sinkJdbcTemplate.update("""
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
                sourceMysqlEndpoint.database(),
                sinkMysqlEndpoint.database(),
                tableName,
                copiedRowCount
        );
    }

    public void markTableError(String tableName, String errorMessage) {
        // Store the latest replication error for the table.
        sinkJdbcTemplate.update("""
                        INSERT INTO replication_table_sync_metadata (
                            source_database, sink_database, table_name, snapshot_status, last_error
                        ) VALUES (?, ?, ?, 'FAILED', ?)
                        ON DUPLICATE KEY UPDATE
                            sink_database = VALUES(sink_database),
                            snapshot_status = VALUES(snapshot_status),
                            last_error = VALUES(last_error)
                        """,
                sourceMysqlEndpoint.database(),
                sinkMysqlEndpoint.database(),
                tableName,
                errorMessage
        );
    }

    public void markIntegrityVerified(String tableName) {
        // Clear the table error after integrity verification succeeds.
        sinkJdbcTemplate.update("""
                        INSERT INTO replication_table_sync_metadata (
                            source_database, sink_database, table_name, last_applied_time, last_error
                        ) VALUES (?, ?, ?, CURRENT_TIMESTAMP(3), NULL)
                        ON DUPLICATE KEY UPDATE
                            sink_database = VALUES(sink_database),
                            last_applied_time = VALUES(last_applied_time),
                            last_error = VALUES(last_error)
                        """,
                sourceMysqlEndpoint.database(),
                sinkMysqlEndpoint.database(),
                tableName
        );
    }

    public void markIntegrityFailure(String tableName, String errorMessage) {
        // Store the integrity verification error for the table.
        sinkJdbcTemplate.update("""
                        INSERT INTO replication_table_sync_metadata (
                            source_database, sink_database, table_name, last_applied_time, last_error
                        ) VALUES (?, ?, ?, CURRENT_TIMESTAMP(3), ?)
                        ON DUPLICATE KEY UPDATE
                            sink_database = VALUES(sink_database),
                            last_applied_time = VALUES(last_applied_time),
                            last_error = VALUES(last_error)
                        """,
                sourceMysqlEndpoint.database(),
                sinkMysqlEndpoint.database(),
                tableName,
                errorMessage
        );
    }

    public void saveTableEventsWithConnection(
            Connection sinkConnection,
            Collection<String> changedTableNames,
            BinlogPosition appliedBinlogPosition
    ) throws Exception {
        Set<String> uniqueChangedTableNames = new LinkedHashSet<>(changedTableNames);

        // Remove invalid table names before writing metadata.
        uniqueChangedTableNames.removeIf(tableName -> tableName == null || tableName.isBlank());

        if (uniqueChangedTableNames.isEmpty()) {
            return;
        }

        // Batch update table-level CDC metadata in the same transaction.
        try (PreparedStatement tableMetadataUpsertStatement = sinkConnection.prepareStatement("""
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

            for (String tableName : uniqueChangedTableNames) {
                tableMetadataUpsertStatement.setString(1, sourceMysqlEndpoint.database());
                tableMetadataUpsertStatement.setString(2, sinkMysqlEndpoint.database());
                tableMetadataUpsertStatement.setString(3, tableName);
                tableMetadataUpsertStatement.setString(4, appliedBinlogPosition.lastEventType());
                tableMetadataUpsertStatement.setString(5, appliedBinlogPosition.binlogFile());

                if (appliedBinlogPosition.binlogPosition() == null) {
                    tableMetadataUpsertStatement.setObject(6, null);
                } else {
                    tableMetadataUpsertStatement.setLong(6, appliedBinlogPosition.binlogPosition());
                }

                tableMetadataUpsertStatement.setString(7, appliedBinlogPosition.gtidSet());
                tableMetadataUpsertStatement.setTimestamp(
                        8,
                        appliedBinlogPosition.lastEventTime() == null
                                ? null
                                : Timestamp.valueOf(appliedBinlogPosition.lastEventTime())
                );

                tableMetadataUpsertStatement.addBatch();
            }

            tableMetadataUpsertStatement.executeBatch();
        }
    }

    public void saveTableEvents(Collection<String> changedTableNames, BinlogPosition appliedBinlogPosition) {
        Set<String> uniqueChangedTableNames = new LinkedHashSet<>(changedTableNames);

        // Remove null or blank table names before updating metadata.
        uniqueChangedTableNames.removeIf(tableName -> tableName == null || tableName.isBlank());

        for (String tableName : uniqueChangedTableNames) {
            // Update metadata for each table touched by the current binlog event.
            sinkJdbcTemplate.update("""
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
                    sourceMysqlEndpoint.database(),
                    sinkMysqlEndpoint.database(),
                    tableName,
                    appliedBinlogPosition.lastEventType(),
                    appliedBinlogPosition.binlogFile(),
                    appliedBinlogPosition.binlogPosition(),
                    appliedBinlogPosition.gtidSet(),
                    appliedBinlogPosition.lastEventTime() == null
                            ? null
                            : Timestamp.valueOf(appliedBinlogPosition.lastEventTime())
            );
        }
    }

    public void refreshStatus(boolean running, boolean paused, String errorMessage) {
        Optional<BinlogPosition> checkpointPositionOptional = load();

        // Read when the latest checkpoint was applied to the sink.
        LocalDateTime lastAppliedTime = sinkJdbcTemplate.query("""
                        SELECT last_applied_time
                        FROM replication_checkpoint
                        WHERE id = ?
                        """,
                preparedStatement -> preparedStatement.setInt(1, SINGLE_CHECKPOINT_ID),
                resultSet -> {
                    if (!resultSet.next()) {
                        return null;
                    }

                    Timestamp lastAppliedTimestamp = resultSet.getTimestamp("last_applied_time");
                    return lastAppliedTimestamp == null ? null : lastAppliedTimestamp.toLocalDateTime();
                }
        );

        BinlogPosition currentCheckpointPosition = checkpointPositionOptional.orElse(
                new BinlogPosition(null, null, null, null, null, null, null)
        );

        // Lag is the difference between now and the latest source event timestamp.
        Long replicationLagInMilliseconds = currentCheckpointPosition.lastEventTime() == null
                ? null
                : Instant.now().toEpochMilli()
                  - currentCheckpointPosition.lastEventTime()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();

        replicationStatusReference.set(new ReplicationStatus(
                running,
                paused,
                sourceMysqlEndpoint.database(),
                sinkMysqlEndpoint.database(),
                currentCheckpointPosition.binlogFile(),
                currentCheckpointPosition.binlogPosition(),
                currentCheckpointPosition.gtidSet(),
                currentCheckpointPosition.lastTableName(),
                currentCheckpointPosition.lastEventType(),
                currentCheckpointPosition.lastEventTime(),
                lastAppliedTime,
                replicationLagInMilliseconds,
                checkpointPositionOptional.isPresent(),
                currentCheckpointPosition.sourceDatabase(),
                errorMessage
        ));
    }

    public ReplicationStatus status() {
        return replicationStatusReference.get();
    }

    public JdbcUrlParser.MysqlEndpoint sourceEndpoint() {
        return sourceMysqlEndpoint;
    }

    public JdbcUrlParser.MysqlEndpoint sinkEndpoint() {
        return sinkMysqlEndpoint;
    }

    public ReplicatorProperties properties() {
        return replicatorProperties;
    }
}
