package com.example.replicator;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@Service
class VerificationService {
    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z0-9_]+");

    private final JdbcTemplate source;
    private final JdbcTemplate sink;
    private final MetadataService metadataService;
    private final CheckpointService checkpointService;
    private final TableFilter tableFilter;
    private final String sourceDb;
    private final String sinkDb;

    VerificationService(@Qualifier("sourceJdbcTemplate") JdbcTemplate source,
                        @Qualifier("sinkJdbcTemplate") JdbcTemplate sink,
                        MetadataService metadataService,
                        CheckpointService checkpointService) {
        this.source = source;
        this.sink = sink;
        this.metadataService = metadataService;
        this.checkpointService = checkpointService;
        this.tableFilter = new TableFilter(checkpointService.properties().getReplication());
        this.sourceDb = checkpointService.sourceEndpoint().database();
        this.sinkDb = checkpointService.sinkEndpoint().database();
    }

    ReplicationVerificationSummary verifyAll(Integer limit) {
        Integer normalizedLimit = limit != null && limit > 0 ? limit : null;
        List<String> tables = source.query("""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_NAME
                        """,
                (rs, rowNum) -> rs.getString(1), sourceDb);
        List<TableVerificationStatus> results = new ArrayList<>();
        for (String table : tables) {
            if (!tableFilter.accepts(table)) {
                continue;
            }
            try {
                results.add(verifyTable(table, normalizedLimit));
            } catch (Exception e) {
                TableVerificationStatus failed = new TableVerificationStatus(table, false, false,
                        0, 0, null, null, null, null, List.of(), List.of(), normalizedLimit,
                        "Verification failed: " + e.getMessage(), Instant.now());
                checkpointService.markIntegrityFailure(table, failed.note());
                results.add(failed);
            }
        }
        int matchedTables = 0;
        int mismatchedTables = 0;
        int notComparableTables = 0;
        for (TableVerificationStatus result : results) {
            if (!result.comparable()) {
                notComparableTables++;
            } else if (result.matched()) {
                matchedTables++;
            } else {
                mismatchedTables++;
            }
        }
        boolean matched = mismatchedTables == 0 && notComparableTables == 0;
        return new ReplicationVerificationSummary(matched, results.size(), matchedTables, mismatchedTables,
                notComparableTables, normalizedLimit, results, Instant.now());
    }

    TableVerificationStatus verifyTable(String table, Integer limit) {
        validateTable(table);
        Integer normalizedLimit = limit != null && limit > 0 ? limit : null;
        TableMetadata metadata = metadataService.table(table);
        if (!metadata.hasPrimaryKey()) {
            TableVerificationStatus result = new TableVerificationStatus(table, false, false, 0, 0, null, null, null, null,
                    metadata.columns(), List.copyOf(metadata.primaryKeys()), normalizedLimit,
                    "Table has no primary key, deterministic checksum ordering is not available", Instant.now());
            checkpointService.markIntegrityFailure(table, result.note());
            return result;
        }
        if (metadata.columns().isEmpty()) {
            TableVerificationStatus result = new TableVerificationStatus(table, false, false, 0, 0, null, null, null, null,
                    metadata.columns(), List.copyOf(metadata.primaryKeys()), normalizedLimit,
                    "Table has no writable columns to verify", Instant.now());
            checkpointService.markIntegrityFailure(table, result.note());
            return result;
        }

        HashResult sourceHash = hash(source, sourceDb, table, metadata, normalizedLimit);
        HashResult sinkHash = hash(sink, sinkDb, table, metadata, normalizedLimit);
        boolean matched = sourceHash.rows() == sinkHash.rows()
                && sourceHash.xorChecksum().equals(sinkHash.xorChecksum())
                && sourceHash.sumChecksum().equals(sinkHash.sumChecksum());
        String note = normalizedLimit == null
                ? "Full table checksum over replicated writable columns"
                : "Limited checksum over first " + normalizedLimit + " rows ordered by primary key";
        TableVerificationStatus result = new TableVerificationStatus(table, true, matched,
                sourceHash.rows(), sinkHash.rows(),
                sourceHash.xorChecksum(), sinkHash.xorChecksum(),
                sourceHash.sumChecksum(), sinkHash.sumChecksum(),
                metadata.columns(), List.copyOf(metadata.primaryKeys()), normalizedLimit, note, Instant.now());
        if (matched) {
            checkpointService.markIntegrityVerified(table);
        } else {
            checkpointService.markIntegrityFailure(table, mismatchNote(result));
        }
        return result;
    }

    private String mismatchNote(TableVerificationStatus result) {
        return "Verification mismatch rows source=" + result.sourceRows()
                + " sink=" + result.sinkRows()
                + " checksum source=" + result.sourceChecksum()
                + " sink=" + result.sinkChecksum()
                + " sum source=" + result.sourceSum()
                + " sink=" + result.sinkSum();
    }

    private void validateTable(String table) {
        if (table == null || table.isBlank() || !SAFE_TABLE_NAME.matcher(table).matches()) {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
        if (!tableFilter.accepts(table)) {
            throw new IllegalArgumentException("Table is excluded by replication filter: " + table);
        }
    }

    private HashResult hash(JdbcTemplate jdbc, String database, String table, TableMetadata metadata, Integer limit) {
        String rowHash = "CRC32(CONCAT_WS(CHAR(31), " + columnExpressions(metadata.columns()) + "))";
        String from = SqlNames.qualified(database, table);
        String sql;
        if (limit == null) {
            sql = "SELECT COUNT(*) AS row_count, "
                    + "COALESCE(BIT_XOR(CAST(" + rowHash + " AS UNSIGNED)), 0) AS xor_checksum, "
                    + "COALESCE(SUM(CAST(" + rowHash + " AS UNSIGNED)), 0) AS sum_checksum "
                    + "FROM " + from;
        } else {
            sql = "SELECT COUNT(*) AS row_count, "
                    + "COALESCE(BIT_XOR(CAST(" + rowHash + " AS UNSIGNED)), 0) AS xor_checksum, "
                    + "COALESCE(SUM(CAST(" + rowHash + " AS UNSIGNED)), 0) AS sum_checksum "
                    + "FROM (SELECT * FROM " + from + " ORDER BY " + orderBy(metadata.primaryKeys())
                    + " LIMIT " + limit + ") v";
        }
        Map<String, Object> row = jdbc.queryForMap(sql);
        return new HashResult(
                ((Number) row.get("row_count")).longValue(),
                String.valueOf(row.get("xor_checksum")),
                String.valueOf(row.get("sum_checksum"))
        );
    }

    private String columnExpressions(List<String> columns) {
        return columns.stream()
                .map(column -> "COALESCE(HEX(" + SqlNames.quote(column) + "), 'NULL')")
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();
    }

    private String orderBy(Iterable<String> primaryKeys) {
        StringBuilder builder = new StringBuilder();
        for (String primaryKey : primaryKeys) {
            if (!builder.isEmpty()) {
                builder.append(", ");
            }
            builder.append(SqlNames.quote(primaryKey)).append(" ASC");
        }
        return builder.toString();
    }

    private record HashResult(long rows, String xorChecksum, String sumChecksum) {
    }
}
