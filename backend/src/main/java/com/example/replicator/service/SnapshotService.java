package com.example.replicator.service;

import com.example.replicator.model.BinlogPosition;
import com.example.replicator.schema.DdlSanitizer;
import com.example.replicator.sql.SqlNames;
import com.example.replicator.sql.TableFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class SnapshotService {
    private static final Logger log = LoggerFactory.getLogger(SnapshotService.class);

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate sinkJdbcTemplate;
    private final DataSource sourceDataSource;
    private final DataSource sinkDataSource;
    private final CheckpointService checkpointService;
    private final TableFilter tableFilter;
    private final String sourceDatabaseName;
    private final String sinkDatabaseName;
    private final boolean strictDdl;

    public SnapshotService(@Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
                           @Qualifier("sinkJdbcTemplate") JdbcTemplate sinkJdbcTemplate,
                           @Qualifier("sourceDataSource") DataSource sourceDataSource,
                           @Qualifier("sinkDataSource") DataSource sinkDataSource,
                           CheckpointService checkpointService) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.sinkJdbcTemplate = sinkJdbcTemplate;
        this.sourceDataSource = sourceDataSource;
        this.sinkDataSource = sinkDataSource;
        this.checkpointService = checkpointService;
        this.tableFilter = new TableFilter(checkpointService.properties().getReplication());
        this.sourceDatabaseName = checkpointService.sourceEndpoint().database();
        this.sinkDatabaseName = checkpointService.sinkEndpoint().database();
        this.strictDdl = checkpointService.properties().getReplication().isStrictDdl();
    }

    public BinlogPosition runIfNeeded() throws Exception {
        var savedCheckpoint = checkpointService.load();
        if (savedCheckpoint.isPresent()
                || !"initial".equalsIgnoreCase(checkpointService.properties().getReplication().getSnapshotMode())) {
            savedCheckpoint.ifPresent(position -> log.info("Resuming replication from checkpoint binlog={}:{} gtid={} lastEventType={} table={}",
                    position.binlogFile(), position.binlogPosition(), position.gtidSet(),
                    position.lastEventType(), position.lastTableName()));
            return savedCheckpoint.orElse(null);
        }

        log.info("No checkpoint found for sourceJdbcTemplate database {}; running initial snapshot", sourceDatabaseName);
        try (Connection lockConnection = sourceDataSource.getConnection()) {
            // Capture a stable binlog position before copying data so CDC can resume after the snapshot.
            boolean sourceReadLockAcquired = tryReadLock(lockConnection);
            if (!sourceReadLockAcquired) {
                log.warn("Source user cannot run FLUSH TABLES WITH READ LOCK; snapshot may be inconsistent if writes occur during copy");
            }
            try {
                BinlogPosition snapshotBinlogPosition = currentSourcePosition(lockConnection);
                createSinkDatabase();
                List<String> snapshotTables = listTables();
                for (String table : snapshotTables) {
                    if (tableFilter.accepts(table)) {
                        createOrReplaceTable(table);
                    }
                }
                for (String table : snapshotTables) {
                    if (tableFilter.accepts(table)) {
                        checkpointService.markSnapshotStarted(table);
                        try {
                            long rowsCopied = copyTable(table);
                            checkpointService.markSnapshotCompleted(table, rowsCopied);
                        } catch (Exception e) {
                            checkpointService.markTableError(table, e.getMessage());
                            throw e;
                        }
                    }
                }
                checkpointService.save(snapshotBinlogPosition);
                log.info("Initial snapshot completed binlog={}:{} gtid={}",
                        snapshotBinlogPosition.binlogFile(), snapshotBinlogPosition.binlogPosition(), snapshotBinlogPosition.gtidSet());
                return snapshotBinlogPosition;
            } finally {
                if (sourceReadLockAcquired) {
                    try (Statement unlockStatement = lockConnection.createStatement()) {
                        unlockStatement.execute("UNLOCK TABLES");
                    }
                }
            }
        }
    }

    private boolean tryReadLock(Connection connection) {
        try (Statement lockStatement = connection.createStatement()) {
            lockStatement.execute("FLUSH TABLES WITH READ LOCK");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private BinlogPosition currentSourcePosition(Connection connection) throws Exception {
        String binlogFile = null;
        Long binlogPosition = null;
        SourceLogStatus logStatus = sourceLogStatus(connection);
        binlogFile = logStatus.file();
        binlogPosition = logStatus.position();
        String gtidSet = null;
        try (Statement gtidStatement = connection.createStatement();
             ResultSet gtidResultSet = gtidStatement.executeQuery("SELECT @@GLOBAL.gtid_executed")) {
            if (gtidResultSet.next()) {
                gtidSet = gtidResultSet.getString(1);
            }
        } catch (Exception e) {
            log.info("GTID is not available on sourceJdbcTemplate: {}", e.getMessage());
        }
        return new BinlogPosition(binlogFile, binlogPosition, gtidSet, "SNAPSHOT", null, LocalDateTime.now());
    }

    private SourceLogStatus sourceLogStatus(Connection connection) throws Exception {
        try {
            return readSourceLogStatus(connection, "SHOW BINARY LOG STATUS");
        } catch (Exception e) {
            log.info("SHOW BINARY LOG STATUS is not available, falling back to SHOW MASTER STATUS: {}", e.getMessage());
            return readSourceLogStatus(connection, "SHOW MASTER STATUS");
        }
    }

    private SourceLogStatus readSourceLogStatus(Connection connection, String statusSql) throws Exception {
        try (Statement statusStatement = connection.createStatement();
             ResultSet statusResultSet = statusStatement.executeQuery(statusSql)) {
            if (!statusResultSet.next()) {
                throw new IllegalStateException(statusSql + " returned no rows; verify log_bin is enabled");
            }
            return new SourceLogStatus(statusResultSet.getString("File"), statusResultSet.getLong("Position"));
        }
    }

    private void createSinkDatabase() {
        sinkJdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS " + SqlNames.quote(sinkDatabaseName));
    }

    private List<String> listTables() {
        List<String> tables = sourceJdbcTemplate.query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
                (resultSet, rowNumber) -> resultSet.getString(1), sourceDatabaseName);
        return orderTablesByForeignKeys(tables);
    }

    private List<String> orderTablesByForeignKeys(List<String> tables) {
        Set<String> tableSet = new LinkedHashSet<>(tables);
        Map<String, Set<String>> dependencies = new HashMap<>();
        for (String table : tables) {
            dependencies.put(table, new LinkedHashSet<>());
        }

        sourceJdbcTemplate.query("""
                        SELECT TABLE_NAME, REFERENCED_TABLE_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = ?
                          AND REFERENCED_TABLE_SCHEMA = ?
                          AND REFERENCED_TABLE_NAME IS NOT NULL
                        """,
                foreignKeyResultSet -> {
                    String table = foreignKeyResultSet.getString("TABLE_NAME");
                    String referencedTable = foreignKeyResultSet.getString("REFERENCED_TABLE_NAME");
                    if (tableSet.contains(table) && tableSet.contains(referencedTable)) {
                        dependencies.get(table).add(referencedTable);
                    }
                },
                sourceDatabaseName, sourceDatabaseName);

        List<String> ordered = new ArrayList<>();
        Set<String> visiting = new HashSet<>();
        Set<String> visited = new HashSet<>();
        for (String table : tables) {
            visitTable(table, dependencies, visiting, visited, ordered);
        }
        return ordered;
    }

    private void visitTable(String table, Map<String, Set<String>> dependencies, Set<String> visiting,
                            Set<String> visited, List<String> ordered) {
        if (visited.contains(table)) {
            return;
        }
        if (!visiting.add(table)) {
            log.warn("Detected circular foreign key dependency while ordering snapshot tables table={}", table);
            return;
        }
        for (String dependency : dependencies.getOrDefault(table, Set.of())) {
            visitTable(dependency, dependencies, visiting, visited, ordered);
        }
        visiting.remove(table);
        visited.add(table);
        ordered.add(table);
    }

    private void createOrReplaceTable(String table) {
        if (sinkTableExists(table)) {
            log.info("Skipped snapshot DDL because sinkJdbcTemplate table already exists table={}", table);
            return;
        }
        Map<String, Object> createTableResult = sourceJdbcTemplate.queryForMap("SHOW CREATE TABLE " + SqlNames.qualified(sourceDatabaseName, table));
        String sourceCreateTableDdl = String.valueOf(createTableResult.get("Create Table"));
        String sinkCreateTableDdl = DdlSanitizer.prepare(sourceCreateTableDdl, sourceDatabaseName, sinkDatabaseName, strictDdl)
                .orElseThrow(() -> new IllegalStateException("CREATE TABLE DDL was skipped for table " + table));
        try {
            sinkJdbcTemplate.execute(sinkCreateTableDdl);
        } catch (RuntimeException e) {
            if (!isTableAlreadyExists(e)) {
                throw e;
            }
            log.info("Skipped snapshot DDL because sinkJdbcTemplate table already exists table={}", table);
        }
        log.info("Applied snapshot DDL table={}", table);
    }

    private boolean sinkTableExists(String table) {
        Integer count = sinkJdbcTemplate.queryForObject("""
                        SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
                        """,
                Integer.class, sinkDatabaseName, table);
        return count != null && count > 0;
    }

    private boolean isTableAlreadyExists(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof SQLException sqlException && sqlException.getErrorCode() == 1050) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private long copyTable(String table) throws Exception {
        List<String> writableColumns = writableColumns(table);
        if (writableColumns.isEmpty()) {
            log.warn("Skipping snapshot copy because table has no writable columns table={}", table);
            return 0;
        }
        String selectColumnsSql = String.join(",", writableColumns.stream().map(SqlNames::quote).toList());
        try (Connection sourceConnection = sourceDataSource.getConnection();
             Connection sinkConnection = sinkDataSource.getConnection();
             PreparedStatement sourceReadStatement = sourceConnection.prepareStatement(
                     "SELECT " + selectColumnsSql + " FROM " + SqlNames.qualified(sourceDatabaseName, table),
                     ResultSet.TYPE_FORWARD_ONLY,
                     ResultSet.CONCUR_READ_ONLY)) {
            // MySQL streams rows only when fetch size is MIN_VALUE on a forward-only result set.
            sourceReadStatement.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet sourceRows = sourceReadStatement.executeQuery()) {
                String valuePlaceholdersSql = String.join(",", writableColumns.stream().map(column -> "?").toList());
                String insertColumnListSql = String.join(",", writableColumns.stream().map(SqlNames::quote).toList());
                String replaceSql = "REPLACE INTO " + SqlNames.qualified(sinkDatabaseName, table)
                        + " (" + insertColumnListSql + ") VALUES (" + valuePlaceholdersSql + ")";
                sinkConnection.setAutoCommit(false);
                int copiedRowCount = 0;
                try (PreparedStatement sinkWriteStatement = sinkConnection.prepareStatement(replaceSql)) {
                    while (sourceRows.next()) {
                        for (int columnNumber = 1; columnNumber <= writableColumns.size(); columnNumber++) {
                            sinkWriteStatement.setObject(columnNumber, sourceRows.getObject(columnNumber));
                        }
                        sinkWriteStatement.addBatch();
                        copiedRowCount++;
                        if (copiedRowCount % 1000 == 0) {
                            sinkWriteStatement.executeBatch();
                            sinkConnection.commit();
                        }
                    }
                    sinkWriteStatement.executeBatch();
                    sinkConnection.commit();
                } catch (Exception e) {
                    sinkConnection.rollback();
                    throw e;
                } finally {
                    sinkConnection.setAutoCommit(true);
                }
                log.info("Copied snapshot table={} rows={}", table, copiedRowCount);
                return copiedRowCount;
            }
        }
    }

    private List<String> writableColumns(String table) {
        return sourceJdbcTemplate.query("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                          AND EXTRA NOT LIKE '%GENERATED%'
                        ORDER BY ORDINAL_POSITION
                        """,
                (resultSet, rowNumber) -> resultSet.getString(1), sourceDatabaseName, table);
    }

    private record SourceLogStatus(String file, Long position) {
    }
}
