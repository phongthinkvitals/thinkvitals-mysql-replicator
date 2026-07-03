package com.example.replicator;

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

    private final JdbcTemplate source;
    private final JdbcTemplate sink;
    private final DataSource sourceDataSource;
    private final DataSource sinkDataSource;
    private final CheckpointService checkpointService;
    private final TableFilter tableFilter;
    private final String sourceDb;
    private final String sinkDb;
    private final boolean strictDdl;

    public SnapshotService(@Qualifier("sourceJdbcTemplate") JdbcTemplate source,
                           @Qualifier("sinkJdbcTemplate") JdbcTemplate sink,
                           @Qualifier("sourceDataSource") DataSource sourceDataSource,
                           @Qualifier("sinkDataSource") DataSource sinkDataSource,
                           CheckpointService checkpointService) {
        this.source = source;
        this.sink = sink;
        this.sourceDataSource = sourceDataSource;
        this.sinkDataSource = sinkDataSource;
        this.checkpointService = checkpointService;
        this.tableFilter = new TableFilter(checkpointService.properties().getReplication());
        this.sourceDb = checkpointService.sourceEndpoint().database();
        this.sinkDb = checkpointService.sinkEndpoint().database();
        this.strictDdl = checkpointService.properties().getReplication().isStrictDdl();
    }

    BinlogPosition runIfNeeded() throws Exception {
        var checkpoint = checkpointService.load();
        if (checkpoint.isPresent()
                || !"initial".equalsIgnoreCase(checkpointService.properties().getReplication().getSnapshotMode())) {
            checkpoint.ifPresent(position -> log.info("Resuming replication from checkpoint binlog={}:{} gtid={} lastEventType={} table={}",
                    position.binlogFile(), position.binlogPosition(), position.gtidSet(),
                    position.lastEventType(), position.lastTableName()));
            return checkpoint.orElse(null);
        }

        log.info("No checkpoint found for source database {}; running initial snapshot", sourceDb);
        try (Connection lockConnection = sourceDataSource.getConnection()) {
            boolean locked = tryReadLock(lockConnection);
            if (!locked) {
                log.warn("Source user cannot run FLUSH TABLES WITH READ LOCK; snapshot may be inconsistent if writes occur during copy");
            }
            try {
                BinlogPosition position = currentSourcePosition(lockConnection);
                createSinkDatabase();
                List<String> tables = listTables();
                for (String table : tables) {
                    if (tableFilter.accepts(table)) {
                        createOrReplaceTable(table);
                    }
                }
                for (String table : tables) {
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
                checkpointService.save(position);
                log.info("Initial snapshot completed binlog={}:{} gtid={}",
                        position.binlogFile(), position.binlogPosition(), position.gtidSet());
                return position;
            } finally {
                if (locked) {
                    try (Statement st = lockConnection.createStatement()) {
                        st.execute("UNLOCK TABLES");
                    }
                }
            }
        }
    }

    private boolean tryReadLock(Connection connection) {
        try (Statement st = connection.createStatement()) {
            st.execute("FLUSH TABLES WITH READ LOCK");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private BinlogPosition currentSourcePosition(Connection connection) throws Exception {
        String file = null;
        Long position = null;
        SourceLogStatus logStatus = sourceLogStatus(connection);
        file = logStatus.file();
        position = logStatus.position();
        String gtid = null;
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery("SELECT @@GLOBAL.gtid_executed")) {
            if (rs.next()) {
                gtid = rs.getString(1);
            }
        } catch (Exception e) {
            log.info("GTID is not available on source: {}", e.getMessage());
        }
        return new BinlogPosition(file, position, gtid, "SNAPSHOT", null, LocalDateTime.now());
    }

    private SourceLogStatus sourceLogStatus(Connection connection) throws Exception {
        try {
            return readSourceLogStatus(connection, "SHOW BINARY LOG STATUS");
        } catch (Exception e) {
            log.info("SHOW BINARY LOG STATUS is not available, falling back to SHOW MASTER STATUS: {}", e.getMessage());
            return readSourceLogStatus(connection, "SHOW MASTER STATUS");
        }
    }

    private SourceLogStatus readSourceLogStatus(Connection connection, String sql) throws Exception {
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) {
                throw new IllegalStateException(sql + " returned no rows; verify log_bin is enabled");
            }
            return new SourceLogStatus(rs.getString("File"), rs.getLong("Position"));
        }
    }

    private void createSinkDatabase() {
        sink.execute("CREATE DATABASE IF NOT EXISTS " + SqlNames.quote(sinkDb));
    }

    private List<String> listTables() {
        List<String> tables = source.query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
                (rs, rowNum) -> rs.getString(1), sourceDb);
        return orderTablesByForeignKeys(tables);
    }

    private List<String> orderTablesByForeignKeys(List<String> tables) {
        Set<String> tableSet = new LinkedHashSet<>(tables);
        Map<String, Set<String>> dependencies = new HashMap<>();
        for (String table : tables) {
            dependencies.put(table, new LinkedHashSet<>());
        }

        source.query("""
                        SELECT TABLE_NAME, REFERENCED_TABLE_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = ?
                          AND REFERENCED_TABLE_SCHEMA = ?
                          AND REFERENCED_TABLE_NAME IS NOT NULL
                        """,
                rs -> {
                    String table = rs.getString("TABLE_NAME");
                    String referencedTable = rs.getString("REFERENCED_TABLE_NAME");
                    if (tableSet.contains(table) && tableSet.contains(referencedTable)) {
                        dependencies.get(table).add(referencedTable);
                    }
                },
                sourceDb, sourceDb);

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
            log.info("Skipped snapshot DDL because sink table already exists table={}", table);
            return;
        }
        Map<String, Object> row = source.queryForMap("SHOW CREATE TABLE " + SqlNames.qualified(sourceDb, table));
        String ddl = String.valueOf(row.get("Create Table"));
        ddl = DdlSanitizer.prepare(ddl, sourceDb, sinkDb, strictDdl)
                .orElseThrow(() -> new IllegalStateException("CREATE TABLE DDL was skipped for table " + table));
        try {
            sink.execute(ddl);
        } catch (RuntimeException e) {
            if (!isTableAlreadyExists(e)) {
                throw e;
            }
            log.info("Skipped snapshot DDL because sink table already exists table={}", table);
        }
        log.info("Applied snapshot DDL table={}", table);
    }

    private boolean sinkTableExists(String table) {
        Integer count = sink.queryForObject("""
                        SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
                        """,
                Integer.class, sinkDb, table);
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
        List<String> columns = writableColumns(table);
        if (columns.isEmpty()) {
            log.warn("Skipping snapshot copy because table has no writable columns table={}", table);
            return 0;
        }
        String selectColumns = String.join(",", columns.stream().map(SqlNames::quote).toList());
        try (Connection sourceConnection = sourceDataSource.getConnection();
             Connection sinkConnection = sinkDataSource.getConnection();
             PreparedStatement read = sourceConnection.prepareStatement(
                     "SELECT " + selectColumns + " FROM " + SqlNames.qualified(sourceDb, table),
                     ResultSet.TYPE_FORWARD_ONLY,
                     ResultSet.CONCUR_READ_ONLY)) {
            read.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = read.executeQuery()) {
                String placeholders = String.join(",", columns.stream().map(c -> "?").toList());
                String columnSql = String.join(",", columns.stream().map(SqlNames::quote).toList());
                String sql = "REPLACE INTO " + SqlNames.qualified(sinkDb, table) + " (" + columnSql + ") VALUES (" + placeholders + ")";
                sinkConnection.setAutoCommit(false);
                int count = 0;
                try (PreparedStatement write = sinkConnection.prepareStatement(sql)) {
                    while (rs.next()) {
                        for (int i = 1; i <= columns.size(); i++) {
                            write.setObject(i, rs.getObject(i));
                        }
                        write.addBatch();
                        count++;
                        if (count % 1000 == 0) {
                            write.executeBatch();
                            sinkConnection.commit();
                        }
                    }
                    write.executeBatch();
                    sinkConnection.commit();
                } catch (Exception e) {
                    sinkConnection.rollback();
                    throw e;
                } finally {
                    sinkConnection.setAutoCommit(true);
                }
                log.info("Copied snapshot table={} rows={}", table, count);
                return count;
            }
        }
    }

    private List<String> writableColumns(String table) {
        return source.query("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                          AND EXTRA NOT LIKE '%GENERATED%'
                        ORDER BY ORDINAL_POSITION
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDb, table);
    }

    private record SourceLogStatus(String file, Long position) {
    }
}
