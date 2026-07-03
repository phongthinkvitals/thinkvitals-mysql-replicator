package com.example.replicator;

import java.util.HashSet;
import java.util.Set;

class TableFilter {
    private final Set<String> includes;
    private final Set<String> excludes;

    TableFilter(ReplicatorProperties.Replication replication) {
        this.includes = new HashSet<>(replication.getIncludeTables());
        this.excludes = new HashSet<>(replication.getExcludeTables());
    }

    boolean accepts(String table) {
        if (!includes.isEmpty() && !includes.contains(table)) {
            return false;
        }
        return !excludes.contains(table);
    }
}
