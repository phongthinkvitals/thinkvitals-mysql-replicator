package com.example.replicator;

import java.time.Instant;
import java.util.List;

record TableVerificationStatus(
        String table,
        boolean comparable,
        boolean matched,
        long sourceRows,
        long sinkRows,
        String sourceChecksum,
        String sinkChecksum,
        String sourceSum,
        String sinkSum,
        List<String> columns,
        List<String> primaryKeys,
        Integer limit,
        String note,
        Instant verifiedAt
) {
}
