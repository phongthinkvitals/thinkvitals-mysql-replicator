package com.example.replicator.sql;

public final class SqlNames {
    private SqlNames() {
    }

    public static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    public static String qualified(String database, String table) {
        return quote(database) + "." + quote(table);
    }
}
