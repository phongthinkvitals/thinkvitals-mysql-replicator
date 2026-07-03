package com.example.replicator;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Builds relaxed sink DDL when full source constraints are too strict for replication.
 * Relaxed mode keeps table columns and the primary key, but skips foreign keys,
 * unique constraints, secondary indexes, fulltext/spatial indexes, and checks.
 */
final class DdlSanitizer {
    private DdlSanitizer() {
    }

    static Optional<String> prepare(String sql, String sourceDb, String sinkDb, boolean strictDdl) {
        String mapped = SchemaMapper.mapDdl(sql, sourceDb, sinkDb);
        if (strictDdl) {
            return Optional.of(mapped);
        }

        String normalized = sql.trim().toUpperCase(Locale.ROOT);
        if (normalized.startsWith("CREATE TABLE")) {
            return Optional.of(relaxCreateTable(mapped));
        }
        if (normalized.startsWith("CREATE INDEX")
                || normalized.startsWith("DROP INDEX")
                || isConstraintOrIndexOnlyAlter(normalized)) {
            return Optional.empty();
        }
        return Optional.of(mapped);
    }

    private static String relaxCreateTable(String sql) {
        int open = sql.indexOf('(');
        if (open < 0) {
            return sql;
        }
        int close = matchingCloseParen(sql, open);
        if (close < 0) {
            return sql;
        }

        List<String> definitions = splitTopLevel(sql.substring(open + 1, close));
        Set<String> primaryKeyColumns = primaryKeyColumns(definitions);
        List<String> kept = new ArrayList<>();
        for (String definition : definitions) {
            if (shouldKeepCreateTableDefinition(definition)) {
                String relaxed = relaxCreateTableDefinition(definition, primaryKeyColumns);
                if (!relaxed.isBlank()) {
                    kept.add(relaxed);
                }
            }
        }
        return sql.substring(0, open + 1)
                + "\n  "
                + String.join(",\n  ", kept)
                + "\n"
                + sql.substring(close, close + 1);
    }

    private static boolean shouldKeepCreateTableDefinition(String definition) {
        String normalized = definition.stripLeading().toUpperCase(Locale.ROOT);
        return normalized.startsWith("PRIMARY KEY") || !isTableConstraintOrIndexDefinition(normalized);
    }

    private static String relaxCreateTableDefinition(String definition, Set<String> primaryKeyColumns) {
        String stripped = definition.strip();
        String normalized = stripped.toUpperCase(Locale.ROOT);
        if (normalized.startsWith("PRIMARY KEY")) {
            return stripped;
        }
        if (isGeneratedColumn(normalized)) {
            return "";
        }
        ColumnDefinition columnDefinition = parseColumnDefinition(stripped);
        if (columnDefinition == null) {
            return stripped;
        }
        String type = typeOnly(columnDefinition.typeAndOptions());
        if (!primaryKeyColumns.contains(columnDefinition.name())) {
            type = relaxedType(type);
        }
        if (type.isBlank()) {
            return "";
        }
        return quoteIdentifier(columnDefinition.name()) + " " + type;
    }

    private static Set<String> primaryKeyColumns(List<String> definitions) {
        Set<String> columns = new LinkedHashSet<>();
        for (String definition : definitions) {
            String stripped = definition.strip();
            if (!stripped.toUpperCase(Locale.ROOT).startsWith("PRIMARY KEY")) {
                continue;
            }
            int open = stripped.indexOf('(');
            int close = open < 0 ? -1 : matchingCloseParen(stripped, open);
            if (open < 0 || close < 0) {
                continue;
            }
            for (String column : splitTopLevel(stripped.substring(open + 1, close))) {
                String value = column.strip();
                if (value.startsWith("`")) {
                    int end = value.indexOf('`', 1);
                    if (end > 0) {
                        columns.add(value.substring(1, end).replace("``", "`"));
                    }
                }
            }
        }
        return columns;
    }

    private static boolean isGeneratedColumn(String normalized) {
        return normalized.contains(" GENERATED ALWAYS ")
                || normalized.contains(" AS (")
                || normalized.endsWith(" STORED")
                || normalized.endsWith(" VIRTUAL");
    }

    private static boolean isTableConstraintOrIndexDefinition(String normalized) {
        return normalized.startsWith("CONSTRAINT ")
                || normalized.startsWith("UNIQUE ")
                || normalized.startsWith("UNIQUE KEY ")
                || normalized.startsWith("UNIQUE INDEX ")
                || normalized.startsWith("KEY ")
                || normalized.startsWith("INDEX ")
                || normalized.startsWith("FULLTEXT ")
                || normalized.startsWith("FULLTEXT KEY ")
                || normalized.startsWith("FULLTEXT INDEX ")
                || normalized.startsWith("SPATIAL ")
                || normalized.startsWith("SPATIAL KEY ")
                || normalized.startsWith("SPATIAL INDEX ")
                || normalized.startsWith("FOREIGN KEY ")
                || normalized.startsWith("CHECK ");
    }

    private static ColumnDefinition parseColumnDefinition(String stripped) {
        if (stripped.startsWith("`")) {
            int closingBacktick = stripped.indexOf('`', 1);
            if (closingBacktick < 0) {
                return null;
            }
            String name = stripped.substring(1, closingBacktick).replace("``", "`");
            return new ColumnDefinition(name, stripped.substring(closingBacktick + 1).stripLeading());
        }

        int end = firstWhitespace(stripped);
        if (end <= 0) {
            return null;
        }
        String name = stripped.substring(0, end);
        return new ColumnDefinition(unquoteIdentifier(name), stripped.substring(end).stripLeading());
    }

    private static int firstWhitespace(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static String quoteIdentifier(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private static String unquoteIdentifier(String value) {
        if (value.startsWith("`") && value.endsWith("`") && value.length() >= 2) {
            return value.substring(1, value.length() - 1).replace("``", "`");
        }
        return value;
    }

    private static String typeOnly(String definition) {
        List<String> tokens = splitByWhitespaceTopLevel(definition);
        List<String> typeTokens = new ArrayList<>();
        for (String token : tokens) {
            if (isColumnOptionKeyword(token)) {
                break;
            }
            typeTokens.add(token);
        }
        return String.join(" ", typeTokens);
    }

    static String relaxedType(String type) {
        String normalized = type.toUpperCase(Locale.ROOT);
        if (normalized.startsWith("VARCHAR")
                || normalized.startsWith("CHAR")
                || normalized.startsWith("TINYTEXT")
                || normalized.startsWith("TEXT")
                || normalized.startsWith("MEDIUMTEXT")
                || normalized.startsWith("LONGTEXT")
                || normalized.startsWith("ENUM")
                || normalized.startsWith("SET")) {
            return "longtext";
        }
        if (normalized.startsWith("VARBINARY")
                || normalized.startsWith("BINARY")
                || normalized.startsWith("TINYBLOB")
                || normalized.startsWith("BLOB")
                || normalized.startsWith("MEDIUMBLOB")
                || normalized.startsWith("LONGBLOB")) {
            return "longblob";
        }
        return type;
    }

    private static boolean isColumnOptionKeyword(String token) {
        String normalized = token.toUpperCase(Locale.ROOT);
        return normalized.equals("NOT")
                || normalized.equals("NULL")
                || normalized.equals("DEFAULT")
                || normalized.equals("AUTO_INCREMENT")
                || normalized.equals("UNIQUE")
                || normalized.equals("PRIMARY")
                || normalized.equals("KEY")
                || normalized.equals("COMMENT")
                || normalized.equals("COLLATE")
                || normalized.equals("CHARACTER")
                || normalized.equals("CHARSET")
                || normalized.equals("REFERENCES")
                || normalized.equals("CHECK")
                || normalized.equals("GENERATED")
                || normalized.equals("AS")
                || normalized.equals("VIRTUAL")
                || normalized.equals("STORED")
                || normalized.equals("VISIBLE")
                || normalized.equals("INVISIBLE")
                || normalized.equals("ON")
                || normalized.equals("COLUMN_FORMAT")
                || normalized.equals("STORAGE");
    }

    private static boolean isConstraintOrIndexOnlyAlter(String normalized) {
        if (!normalized.startsWith("ALTER TABLE")) {
            return false;
        }
        boolean columnChange = normalized.contains(" ADD COLUMN ")
                || normalized.contains(" MODIFY COLUMN ")
                || normalized.contains(" CHANGE COLUMN ")
                || normalized.contains(" DROP COLUMN ")
                || normalized.contains(" RENAME COLUMN ");
        if (columnChange) {
            return false;
        }
        return normalized.contains(" ADD CONSTRAINT ")
                || normalized.contains(" DROP CONSTRAINT ")
                || normalized.contains(" ADD FOREIGN KEY ")
                || normalized.contains(" DROP FOREIGN KEY ")
                || normalized.contains(" ADD UNIQUE ")
                || normalized.contains(" ADD KEY ")
                || normalized.contains(" ADD INDEX ")
                || normalized.contains(" DROP KEY ")
                || normalized.contains(" DROP INDEX ")
                || normalized.contains(" ADD FULLTEXT ")
                || normalized.contains(" ADD SPATIAL ")
                || normalized.contains(" ADD CHECK ")
                || normalized.contains(" DROP CHECK ");
    }

    private static int matchingCloseParen(String sql, int open) {
        int depth = 0;
        char quote = 0;
        for (int i = open; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                if (c == quote && (quote == '`' || i == 0 || sql.charAt(i - 1) != '\\')) {
                    quote = 0;
                }
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static List<String> splitTopLevel(String body) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        char quote = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (quote != 0) {
                if (c == quote && (quote == '`' || i == 0 || body.charAt(i - 1) != '\\')) {
                    quote = 0;
                }
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(body.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(body.substring(start));
        return parts;
    }

    private static List<String> splitByWhitespaceTopLevel(String value) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = -1;
        char quote = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (quote != 0) {
                if (c == quote && (quote == '`' || i == 0 || value.charAt(i - 1) != '\\')) {
                    quote = 0;
                }
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                if (start < 0) {
                    start = i;
                }
            } else if (c == '(') {
                depth++;
                if (start < 0) {
                    start = i;
                }
            } else if (c == ')') {
                depth--;
            } else if (Character.isWhitespace(c) && depth == 0) {
                if (start >= 0) {
                    parts.add(value.substring(start, i));
                    start = -1;
                }
            } else if (start < 0) {
                start = i;
            }
        }
        if (start >= 0) {
            parts.add(value.substring(start));
        }
        return parts;
    }

    private record ColumnDefinition(String name, String typeAndOptions) {
    }
}
