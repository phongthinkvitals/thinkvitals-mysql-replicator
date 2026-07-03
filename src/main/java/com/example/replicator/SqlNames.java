package com.example.replicator;

final class SqlNames {
    private SqlNames() {
    }

    static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    static String qualified(String database, String table) {
        return quote(database) + "." + quote(table);
    }
}
