package com.example.replicator;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
class MetadataService {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);

    private final JdbcTemplate source;
    private final JdbcTemplate sink;
    private final String sourceDb;
    private final String sinkDb;
    private final boolean strictDdl;
    private final Map<String, TableMetadata> cache = new ConcurrentHashMap<>();
    private final Set<String> ensuredSinkTables = ConcurrentHashMap.newKeySet();

    MetadataService(@Qualifier("sourceJdbcTemplate") JdbcTemplate source,
                    @Qualifier("sinkJdbcTemplate") JdbcTemplate sink,
                    CheckpointService checkpointService) {
        this.source = source;
        this.sink = sink;
        this.sourceDb = checkpointService.sourceEndpoint().database();
        this.sinkDb = checkpointService.sinkEndpoint().database();
        this.strictDdl = checkpointService.properties().getReplication().isStrictDdl();
    }

    TableMetadata table(String table) {
        return cache.computeIfAbsent(table, this::load);
    }

    void ensureSinkTable(String table) {
        if (ensuredSinkTables.contains(table)) {
            return;
        }
        synchronized (ensuredSinkTables) {
            if (ensuredSinkTables.contains(table)) {
                return;
            }
            if (!sinkTableExists(table)) {
                Map<String, Object> row = source.queryForMap("SHOW CREATE TABLE " + SqlNames.qualified(sourceDb, table));
                String ddl = String.valueOf(row.get("Create Table"));
                String mapped = DdlSanitizer.prepare(ddl, sourceDb, sinkDb, strictDdl)
                        .orElseThrow(() -> new IllegalStateException("CREATE TABLE DDL was skipped for table " + table));
                sink.execute(mapped);
                invalidate(table);
                log.info("Created missing sink table from source DDL table={} ddl=\"{}\"", table, mapped);
            }
            ensuredSinkTables.add(table);
        }
    }

    void widenSinkColumn(String table, String column) {
        List<String> types = source.query("""
                        SELECT COLUMN_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDb, table, column);
        if (types.isEmpty()) {
            throw new IllegalStateException("Cannot widen missing source column " + table + "." + column);
        }
        String relaxedType = DdlSanitizer.relaxedType(types.get(0));
        if (relaxedType.equalsIgnoreCase(types.get(0))) {
            throw new IllegalStateException("No relaxed wider type available for column " + table + "." + column
                    + " sourceType=" + types.get(0));
        }
        sink.execute("ALTER TABLE " + SqlNames.qualified(sinkDb, table)
                + " MODIFY COLUMN " + SqlNames.quote(column) + " " + relaxedType);
        invalidate(table);
        log.warn("Widened sink column after truncation table={} column={} type={}", table, column, relaxedType);
    }

    void invalidate(String table) {
        if (table == null) {
            cache.clear();
            ensuredSinkTables.clear();
        } else {
            cache.remove(table);
            ensuredSinkTables.remove(table);
        }
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

    private TableMetadata load(String table) {
        List<String> columns = source.query("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                          AND EXTRA NOT LIKE '%GENERATED%'
                        ORDER BY ORDINAL_POSITION
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDb, table);
        Set<String> primaryKeys = new LinkedHashSet<>(source.query("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
                        ORDER BY ORDINAL_POSITION
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDb, table));
        return new TableMetadata(table, columns, primaryKeys);
    }
}
