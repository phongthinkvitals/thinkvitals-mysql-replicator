package com.example.replicator.service;

import com.example.replicator.metadata.MetadataService;
import com.example.replicator.model.ReplicationVerificationSummary;
import com.example.replicator.model.TableMetadata;
import com.example.replicator.model.TableVerificationStatus;
import com.example.replicator.sql.SqlNames;
import com.example.replicator.sql.TableFilter;
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
public class VerificationService {
    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z0-9_]+");

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate sinkJdbcTemplate;
    private final MetadataService metadataService;
    private final CheckpointService checkpointService;
    private final TableFilter tableFilter;
    private final String sourceDatabaseName;
    private final String sinkDatabaseName;

    public VerificationService(@Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
                        @Qualifier("sinkJdbcTemplate") JdbcTemplate sinkJdbcTemplate,
                        MetadataService metadataService,
                        CheckpointService checkpointService) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.sinkJdbcTemplate = sinkJdbcTemplate;
        this.metadataService = metadataService;
        this.checkpointService = checkpointService;
        this.tableFilter = new TableFilter(checkpointService.properties().getReplication());
        this.sourceDatabaseName = checkpointService.sourceEndpoint().database();
        this.sinkDatabaseName = checkpointService.sinkEndpoint().database();
    }

    public ReplicationVerificationSummary verifyAll(Integer limit) {
        Integer normalizedLimit = limit != null && limit > 0 ? limit : null;
        List<String> tables = sourceJdbcTemplate.query("""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_NAME
                        """,
                (resultSet, rowNumber) -> resultSet.getString(1), sourceDatabaseName);
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

    public TableVerificationStatus verifyTable(String table, Integer limit) {
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

        HashResult sourceHash = hash(sourceJdbcTemplate, sourceDatabaseName, table, metadata, normalizedLimit);
        HashResult sinkHash = hash(sinkJdbcTemplate, sinkDatabaseName, table, metadata, normalizedLimit);
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
        return "Verification mismatch rows sourceJdbcTemplate=" + result.sourceRows()
                + " sinkJdbcTemplate=" + result.sinkRows()
                + " checksum sourceJdbcTemplate=" + result.sourceChecksum()
                + " sinkJdbcTemplate=" + result.sinkChecksum()
                + " sum sourceJdbcTemplate=" + result.sourceSum()
                + " sinkJdbcTemplate=" + result.sinkSum();
    }

    private void validateTable(String table) {
        if (table == null || table.isBlank() || !SAFE_TABLE_NAME.matcher(table).matches()) {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
        if (!tableFilter.accepts(table)) {
            throw new IllegalArgumentException("Table is excluded by replication filter: " + table);
        }
    }

    private HashResult hash(JdbcTemplate jdbcTemplate, String databaseName, String table, TableMetadata tableMetadata, Integer limit) {
        // CHAR(31) is a low-risk separator for deterministic row checksums across source and sink.
        String rowChecksumExpression = "CRC32(CONCAT_WS(CHAR(31), " + columnExpressions(tableMetadata.columns()) + "))";
        String qualifiedTableName = SqlNames.qualified(databaseName, table);
        String checksumSql;
        if (limit == null) {
            checksumSql = "SELECT COUNT(*) AS row_count, "
                    + "COALESCE(BIT_XOR(CAST(" + rowChecksumExpression + " AS UNSIGNED)), 0) AS xor_checksum, "
                    + "COALESCE(SUM(CAST(" + rowChecksumExpression + " AS UNSIGNED)), 0) AS sum_checksum "
                    + "FROM " + qualifiedTableName;
        } else {
            checksumSql = "SELECT COUNT(*) AS row_count, "
                    + "COALESCE(BIT_XOR(CAST(" + rowChecksumExpression + " AS UNSIGNED)), 0) AS xor_checksum, "
                    + "COALESCE(SUM(CAST(" + rowChecksumExpression + " AS UNSIGNED)), 0) AS sum_checksum "
                    + "FROM (SELECT * FROM " + qualifiedTableName + " ORDER BY " + orderBy(tableMetadata.primaryKeys())
                    + " LIMIT " + limit + ") v";
        }
        Map<String, Object> checksumRow = jdbcTemplate.queryForMap(checksumSql);
        return new HashResult(
                ((Number) checksumRow.get("row_count")).longValue(),
                String.valueOf(checksumRow.get("xor_checksum")),
                String.valueOf(checksumRow.get("sum_checksum"))
        );
    }

    private String columnExpressions(List<String> columns) {
        return columns.stream()
                .map(column -> "COALESCE(HEX(" + SqlNames.quote(column) + "), 'NULL')")
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();
    }

    private String orderBy(Iterable<String> primaryKeys) {
        StringBuilder orderByClause = new StringBuilder();
        for (String primaryKey : primaryKeys) {
            if (!orderByClause.isEmpty()) {
                orderByClause.append(", ");
            }
            orderByClause.append(SqlNames.quote(primaryKey)).append(" ASC");
        }
        return orderByClause.toString();
    }

    private record HashResult(long rows, String xorChecksum, String sumChecksum) {
    }
}
