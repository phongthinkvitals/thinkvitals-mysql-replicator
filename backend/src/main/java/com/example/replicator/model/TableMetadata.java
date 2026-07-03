package com.example.replicator.model;

import java.util.List;
import java.util.Set;

public record TableMetadata(String table, List<String> columns, Set<String> primaryKeys, List<String> sourceColumns) {
    public boolean hasPrimaryKey() {
        return !primaryKeys.isEmpty();
    }

    public int sourceIndex(String column) {
        int index = sourceColumns.indexOf(column);
        if (index < 0) {
            throw new IllegalStateException("Column " + column + " not found in source table " + table);
        }
        return index;
    }
}
