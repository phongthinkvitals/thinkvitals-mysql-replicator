package com.example.replicator;

import java.util.List;
import java.util.Set;

record TableMetadata(String table, List<String> columns, Set<String> primaryKeys) {
    boolean hasPrimaryKey() {
        return !primaryKeys.isEmpty();
    }
}
