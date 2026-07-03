package com.example.replicator;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
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
        this.tableFilter = new TableFilter(checkpointService.properties().getReplication());
        this.sourceDb = checkpointService.sourceEndpoint().database();
        this.sinkDb = checkpointService.sinkEndpoint().database();
    }

    TableVerificationStatus verifyTable(String table, Integer limit) {
        validateTable(table);
        Integer normalizedLimit = limit != null && limit > 0 ? limit : null;
        TableMetadata metadata = metadataService.table(table);
        if (!metadata.hasPrimaryKey()) {
            return new TableVerificationStatus(table, false, false, 0, 0, null, null, null, null,
                    metadata.columns(), List.copyOf(metadata.primaryKeys()), normalizedLimit,
                    "Table has no primary key, deterministic checksum ordering is not available", Instant.now());
        }
        if (metadata.columns().isEmpty()) {
            return new TableVerificationStatus(table, false, false, 0, 0, null, null, null, null,
                    metadata.columns(), List.copyOf(metadata.primaryKeys()), normalizedLimit,
                    "Table has no writable columns to verify", Instant.now());
        }

        HashResult sourceHash = hash(source, sourceDb, table, metadata, normalizedLimit);
        HashResult sinkHash = hash(sink, sinkDb, table, metadata, normalizedLimit);
        boolean matched = sourceHash.rows() == sinkHash.rows()
                && sourceHash.xorChecksum().equals(sinkHash.xorChecksum())
                && sourceHash.sumChecksum().equals(sinkHash.sumChecksum());
        String note = normalizedLimit == null
                ? "Full table checksum over replicated writable columns"
                : "Limited checksum over first " + normalizedLimit + " rows ordered by primary key";
        return new TableVerificationStatus(table, true, matched,
                sourceHash.rows(), sinkHash.rows(),
                sourceHash.xorChecksum(), sinkHash.xorChecksum(),
                sourceHash.sumChecksum(), sinkHash.sumChecksum(),
                metadata.columns(), List.copyOf(metadata.primaryKeys()), normalizedLimit, note, Instant.now());
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
