package com.example.replicator.schema;

import com.example.replicator.sql.SqlNames;

import java.util.regex.Pattern;

public class SchemaMapper {
    private SchemaMapper() {
    }

    public static String mapDdl(String sql, String sourceDatabaseName, String sinkDatabaseName) {
        String mapped = sql;
        mapped = mapped.replace(SqlNames.quote(sourceDatabaseName) + ".", SqlNames.quote(sinkDatabaseName) + ".");
        mapped = mapped.replaceAll("(?i)\\b" + Pattern.quote(sourceDatabaseName) + "\\.", sinkDatabaseName + ".");
        return mapped;
    }
}
