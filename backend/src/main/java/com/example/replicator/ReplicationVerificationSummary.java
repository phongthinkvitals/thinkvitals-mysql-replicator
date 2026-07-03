package com.example.replicator;

import java.time.Instant;
import java.util.List;

record ReplicationVerificationSummary(
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
