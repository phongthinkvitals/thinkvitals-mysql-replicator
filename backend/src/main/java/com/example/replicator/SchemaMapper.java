package com.example.replicator;

import java.util.regex.Pattern;

class SchemaMapper {
    private SchemaMapper() {
    }

    static String mapDdl(String sql, String sourceDb, String sinkDb) {
        String mapped = sql;
        mapped = mapped.replace(SqlNames.quote(sourceDb) + ".", SqlNames.quote(sinkDb) + ".");
        mapped = mapped.replaceAll("(?i)\\b" + Pattern.quote(sourceDb) + "\\.", sinkDb + ".");
        return mapped;
    }
}
