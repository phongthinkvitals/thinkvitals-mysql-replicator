package com.example.replicator.sql;

import com.example.replicator.config.ReplicatorProperties;

import java.util.HashSet;
import java.util.Set;

public class TableFilter {
    private final Set<String> includes;
    private final Set<String> excludes;

    public TableFilter(ReplicatorProperties.Replication replication) {
        this.includes = new HashSet<>(replication.getIncludeTables());
        this.excludes = new HashSet<>(replication.getExcludeTables());
    }

    public boolean accepts(String table) {
        if (!includes.isEmpty() && !includes.contains(table)) {
            return false;
        }
        return !excludes.contains(table);
    }
}
