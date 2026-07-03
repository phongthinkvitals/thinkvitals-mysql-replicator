package com.example.replicator.metadata;

import com.example.replicator.model.TableMetadata;
import com.example.replicator.schema.DdlSanitizer;
import com.example.replicator.service.CheckpointService;
import com.example.replicator.sql.SqlNames;
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
public class MetadataService {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate sinkJdbcTemplate;
    private final String sourceDatabaseName;
    private final String sinkDatabaseName;
    private final boolean strictDdl;
    private final Map<String, TableMetadata> cache = new ConcurrentHashMap<>();
    private final Set<String> ensuredSinkTables = ConcurrentHashMap.newKeySet();

    public MetadataService(@Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
                    @Qualifier("sinkJdbcTemplate") JdbcTemplate sinkJdbcTemplate,
                    CheckpointService checkpointService) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.sinkJdbcTemplate = sinkJdbcTemplate;
        this.sourceDatabaseName = checkpointService.sourceEndpoint().database();
        this.sinkDatabaseName = checkpointService.sinkEndpoint().database();
        this.strictDdl = checkpointService.properties().getReplication().isStrictDdl();
    }

    public TableMetadata table(String table) {
        return cache.computeIfAbsent(table, this::load);
    }

    public void ensureSinkTable(String table) {
        if (ensuredSinkTables.contains(table)) {
            return;
        }
        synchronized (ensuredSinkTables) {
            if (ensuredSinkTables.contains(table)) {
                return;
            }
            if (!sinkTableExists(table)) {
                Map<String, Object> row = sourceJdbcTemplate.queryForMap("SHOW CREATE TABLE " + SqlNames.qualified(sourceDatabaseName, table));
                String ddl = String.valueOf(row.get("Create Table"));
                String mapped = DdlSanitizer.prepare(ddl, sourceDatabaseName, sinkDatabaseName, strictDdl)
                        .orElseThrow(() -> new IllegalStateException("CREATE TABLE DDL was skipped for table " + table));
                sinkJdbcTemplate.execute(mapped);
                invalidate(table);
                log.info("Created missing sinkJdbcTemplate table from sourceJdbcTemplate DDL table={} ddl=\"{}\"", table, mapped);
            }
            ensuredSinkTables.add(table);
        }
    }

    public void widenSinkColumn(String table, String column) {
        List<String> types = sourceJdbcTemplate.query("""
                        SELECT COLUMN_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDatabaseName, table, column);
        if (types.isEmpty()) {
            throw new IllegalStateException("Cannot widen missing sourceJdbcTemplate column " + table + "." + column);
        }
        String relaxedType = DdlSanitizer.relaxedType(types.get(0));
        if (relaxedType.equalsIgnoreCase(types.get(0))) {
            throw new IllegalStateException("No relaxed wider type available for column " + table + "." + column
                    + " sourceType=" + types.get(0));
        }
        sinkJdbcTemplate.execute("ALTER TABLE " + SqlNames.qualified(sinkDatabaseName, table)
                + " MODIFY COLUMN " + SqlNames.quote(column) + " " + relaxedType);
        invalidate(table);
        log.warn("Widened sinkJdbcTemplate column after truncation table={} column={} type={}", table, column, relaxedType);
    }

    public void invalidate(String table) {
        if (table == null) {
            cache.clear();
            ensuredSinkTables.clear();
        } else {
            cache.remove(table);
            ensuredSinkTables.remove(table);
        }
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

    private TableMetadata load(String table) {
        List<String> sourceColumns = sourceJdbcTemplate.query("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                        ORDER BY ORDINAL_POSITION
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDatabaseName, table);
        List<String> columns = sourceJdbcTemplate.query("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                          AND EXTRA NOT LIKE '%GENERATED%'
                        ORDER BY ORDINAL_POSITION
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDatabaseName, table);
        Set<String> primaryKeys = new LinkedHashSet<>(sourceJdbcTemplate.query("""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
                        ORDER BY ORDINAL_POSITION
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDatabaseName, table));
        return new TableMetadata(table, columns, primaryKeys, sourceColumns);
    }
}
