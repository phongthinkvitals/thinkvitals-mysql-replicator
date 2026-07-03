package com.example.replicator;

import java.util.List;
import java.util.Set;

record TableMetadata(String table, List<String> columns, Set<String> primaryKeys, List<String> sourceColumns) {
    boolean hasPrimaryKey() {
        return !primaryKeys.isEmpty();
    }

    int sourceIndex(String column) {
        int index = sourceColumns.indexOf(column);
        if (index < 0) {
            throw new IllegalStateException("Column " + column + " not found in source table " + table);
        }
        return index;
    }
}
