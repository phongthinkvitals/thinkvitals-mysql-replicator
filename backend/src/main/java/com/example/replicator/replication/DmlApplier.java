package com.example.replicator.replication;

import com.example.replicator.metadata.MetadataService;
import com.example.replicator.model.BinlogPosition;
import com.example.replicator.model.TableMetadata;
import com.example.replicator.service.CheckpointService;
import com.example.replicator.sql.SqlNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DmlApplier {
    private static final Logger log = LoggerFactory.getLogger(DmlApplier.class);
    private static final Pattern DATA_TOO_LONG_COLUMN = Pattern.compile("Data too long for column '([^']+)'");

    private final DataSource sinkDataSource;
    private final JdbcTemplate sinkJdbcTemplate;
    private final CheckpointService checkpointService;
    private final MetadataService metadataService;
    private final String sinkDatabaseName;

    public DmlApplier(@Qualifier("sinkDataSource") DataSource sinkDataSource,
               @Qualifier("sinkJdbcTemplate") JdbcTemplate sinkJdbcTemplate,
               CheckpointService checkpointService,
               MetadataService metadataService) {
        this.sinkDataSource = sinkDataSource;
        this.sinkJdbcTemplate = sinkJdbcTemplate;
        this.checkpointService = checkpointService;
        this.metadataService = metadataService;
        this.sinkDatabaseName = checkpointService.sinkEndpoint().database();
    }

    public void applyTransaction(List<RowChange> changes, BinlogPosition commitPosition) throws Exception {
        if (changes.isEmpty()) {
            checkpointService.save(commitPosition);
            return;
        }
        long start = System.currentTimeMillis();
        try (Connection connection = sinkDataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                for (RowChange change : changes) {
                    applyOne(connection, change);
                }
                checkpointService.saveWithConnection(connection, commitPosition);
                checkpointService.saveTableEventsWithConnection(connection, changedTables(changes), commitPosition);
                connection.commit();
                long duration = System.currentTimeMillis() - start;
                RowChange last = changes.get(changes.size() - 1);
                log.info("Applied transaction events={} lastType={} table={} binlog={}:{} durationMs={}",
                        changes.size(), last.kind(), last.table(), commitPosition.binlogFile(),
                        commitPosition.binlogPosition(), duration);
            } catch (Exception e) {
                connection.rollback();
                for (String table : changedTables(changes)) {
                    checkpointService.markTableError(table, e.getMessage());
                }
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
        checkpointService.refreshStatus(true, false, null);
    }

    private Set<String> changedTables(List<RowChange> changes) {
        Set<String> tables = new LinkedHashSet<>();
        for (RowChange change : changes) {
            tables.add(change.table());
        }
        return tables;
    }

    private void applyOne(Connection connection, RowChange change) throws Exception {
        metadataService.ensureSinkTable(change.table());
        TableMetadata metadata = metadataService.table(change.table());
        if (metadata.columns().isEmpty()) {
            throw new IllegalStateException("No source columns found for table " + change.table()
                    + " at binlog " + change.position().binlogFile() + ":" + change.position().binlogPosition());
        }
        try {
            applyOne(connection, change, metadata);
        } catch (Exception e) {
            if (isMissingTable(e)) {
                log.warn("Sink table is missing table={}, recreating from source DDL before applying {}", change.table(), change.kind());
                metadataService.invalidate(change.table());
                metadataService.ensureSinkTable(change.table());
                applyOne(connection, change, metadataService.table(change.table()));
                return;
            }
            String truncatedColumn = truncatedColumn(e);
            if (truncatedColumn != null && !checkpointService.properties().getReplication().isStrictDdl()) {
                metadataService.widenSinkColumn(change.table(), truncatedColumn);
                applyOne(connection, change, metadataService.table(change.table()));
                return;
            }
            {
                throw e;
            }
        }
    }

    private void applyOne(Connection connection, RowChange change, TableMetadata metadata) throws Exception {
        if (change.kind() != RowChange.Kind.INSERT
                && checkpointService.properties().getReplication().isRequirePrimaryKey()
                && !metadata.hasPrimaryKey()) {
            throw new IllegalStateException("Missing primary key for table " + change.table());
        }
        switch (change.kind()) {
            case INSERT -> applyInsert(connection, metadata, change.after());
            case UPDATE -> applyUpdate(connection, metadata, change.before(), change.after());
            case DELETE -> applyDelete(connection, metadata, change.before());
        }
    }

    private boolean isMissingTable(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof SQLException sqlException
                    && ("42S02".equals(sqlException.getSQLState()) || sqlException.getErrorCode() == 1146)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String truncatedColumn(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof SQLException sqlException
                    && (sqlException.getErrorCode() == 1406 || "22001".equals(sqlException.getSQLState()))) {
                Matcher matcher = DATA_TOO_LONG_COLUMN.matcher(sqlException.getMessage());
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
            current = current.getCause();
        }
        return null;
    }

    private void applyInsert(Connection connection, TableMetadata metadata, Object[] values) throws Exception {
        String columns = String.join(",", metadata.columns().stream().map(SqlNames::quote).toList());
        String placeholders = String.join(",", metadata.columns().stream().map(c -> "?").toList());
        String sql = "INSERT INTO " + SqlNames.qualified(sinkDatabaseName, metadata.table()) + " (" + columns + ") VALUES (" + placeholders + ")";
        if ("upsert".equalsIgnoreCase(checkpointService.properties().getReplication().getOnDuplicateInsert())) {
            StringJoiner update = new StringJoiner(",");
            for (String column : metadata.columns()) {
                update.add(SqlNames.quote(column) + " = VALUES(" + SqlNames.quote(column) + ")");
            }
            sql += " ON DUPLICATE KEY UPDATE " + update;
        }
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bindValues(ps, metadata, values, 1);
            ps.executeUpdate();
        }
    }

    private void applyUpdate(Connection connection, TableMetadata metadata, Object[] before, Object[] after) throws Exception {
        StringJoiner set = new StringJoiner(",");
        for (String column : metadata.columns()) {
            if (!metadata.primaryKeys().contains(column)) {
                set.add(SqlNames.quote(column) + " = ?");
            }
        }
        String where = pkWhere(metadata);
        String sql = "UPDATE " + SqlNames.qualified(sinkDatabaseName, metadata.table()) + " SET " + set + " WHERE " + where;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int index = 1;
            for (String column : metadata.columns()) {
                if (!metadata.primaryKeys().contains(column)) {
                    ps.setObject(index++, normalize(after[metadata.sourceIndex(column)]));
                }
            }
            bindPrimaryKeys(ps, metadata, before, index);
            int affected = ps.executeUpdate();
            if (affected == 0) {
                log.warn("UPDATE found no sinkJdbcTemplate row table={}, applying after image as insert", metadata.table());
                applyInsert(connection, metadata, after);
            }
        }
    }

    private void applyDelete(Connection connection, TableMetadata metadata, Object[] before) throws Exception {
        String sql = "DELETE FROM " + SqlNames.qualified(sinkDatabaseName, metadata.table()) + " WHERE " + pkWhere(metadata);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            bindPrimaryKeys(ps, metadata, before, 1);
            int affected = ps.executeUpdate();
            if (affected == 0) {
                log.warn("DELETE found no sinkJdbcTemplate row table={}", metadata.table());
            }
        }
    }

    private String pkWhere(TableMetadata metadata) {
        StringJoiner where = new StringJoiner(" AND ");
        for (String pk : metadata.primaryKeys()) {
            where.add(SqlNames.quote(pk) + " = ?");
        }
        return where.toString();
    }

    private void bindPrimaryKeys(PreparedStatement ps, TableMetadata metadata, Object[] values, int startIndex) throws Exception {
        int index = startIndex;
        for (String pk : metadata.primaryKeys()) {
            int columnIndex = metadata.sourceIndex(pk);
            ps.setObject(index++, normalize(values[columnIndex]));
        }
    }

    private void bindValues(PreparedStatement ps, TableMetadata metadata, Object[] values, int startIndex) throws Exception {
        int index = startIndex;
        for (String column : metadata.columns()) {
            ps.setObject(index++, normalize(values[metadata.sourceIndex(column)]));
        }
    }

    private Object normalize(Object value) {
        if (value instanceof LocalDate || value instanceof LocalTime || value instanceof LocalDateTime) {
            return value;
        }
        if (value instanceof java.util.Date date) {
            return new Timestamp(date.getTime());
        }
        if (value instanceof BitSet bitSet) {
            List<Integer> bits = new ArrayList<>();
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                bits.add(i + 1);
            }
            return bits.toString();
        }
        return value;
    }
}
