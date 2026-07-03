package com.example.replicator.model;

import java.time.Instant;
import java.util.List;

public record ReplicationVerificationSummary(
        boolean matched,
        int totalTables,
        int matchedTables,
        int mismatchedTables,
        int notComparableTables,
        Integer limit,
        List<TableVerificationStatus> tables,
        Instant verifiedAt
) {
}
