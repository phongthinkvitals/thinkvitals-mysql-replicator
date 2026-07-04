package com.example.replicator.schema;

import com.example.replicator.sql.SqlNames;

import java.util.regex.Pattern;

public class SchemaMapper {
    private SchemaMapper() {
    }

    public static String mapDdl(String sourceDdlSql, String sourceDatabaseName, String sinkDatabaseName) {
        String sinkDdlSql = sourceDdlSql;
        sinkDdlSql = sinkDdlSql.replace(SqlNames.quote(sourceDatabaseName) + ".", SqlNames.quote(sinkDatabaseName) + ".");
        sinkDdlSql = sinkDdlSql.replaceAll("(?i)\\b" + Pattern.quote(sourceDatabaseName) + "\\.", sinkDatabaseName + ".");
        return sinkDdlSql;
    }
}
