package com.example.replicator.schema;

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
public final class DdlSanitizer {
    private DdlSanitizer() {
    }

    public static Optional<String> prepare(String sourceDdlSql, String sourceDatabaseName, String sinkDatabaseName, boolean strictDdl) {
        String mappedSinkDdlSql = SchemaMapper.mapDdl(sourceDdlSql, sourceDatabaseName, sinkDatabaseName);
        if (strictDdl) {
            return Optional.of(mappedSinkDdlSql);
        }

        String normalizedSourceDdlSql = sourceDdlSql.trim().toUpperCase(Locale.ROOT);
        if (normalizedSourceDdlSql.startsWith("CREATE TABLE")) {
            return Optional.of(relaxCreateTable(mappedSinkDdlSql));
        }
        if (normalizedSourceDdlSql.startsWith("CREATE INDEX")
                || normalizedSourceDdlSql.startsWith("DROP INDEX")
                || isConstraintOrIndexOnlyAlter(normalizedSourceDdlSql)) {
            return Optional.empty();
        }
        return Optional.of(mappedSinkDdlSql);
    }

    private static String relaxCreateTable(String createTableSql) {
        int openingParenthesisIndex = createTableSql.indexOf('(');
        if (openingParenthesisIndex < 0) {
            return createTableSql;
        }
        int closingParenthesisIndex = matchingCloseParen(createTableSql, openingParenthesisIndex);
        if (closingParenthesisIndex < 0) {
            return createTableSql;
        }

        List<String> tableDefinitions = splitTopLevel(createTableSql.substring(openingParenthesisIndex + 1, closingParenthesisIndex));
        Set<String> primaryKeyColumns = primaryKeyColumns(tableDefinitions);
        List<String> keptDefinitions = new ArrayList<>();
        for (String tableDefinition : tableDefinitions) {
            if (shouldKeepCreateTableDefinition(tableDefinition)) {
                String relaxedDefinition = relaxCreateTableDefinition(tableDefinition, primaryKeyColumns);
                if (!relaxedDefinition.isBlank()) {
                    keptDefinitions.add(relaxedDefinition);
                }
            }
        }
        return createTableSql.substring(0, openingParenthesisIndex + 1)
                + "\n  "
                + String.join(",\n  ", keptDefinitions)
                + "\n"
                + createTableSql.substring(closingParenthesisIndex, closingParenthesisIndex + 1);
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
        String columnType = typeOnly(columnDefinition.typeAndOptions());
        if (!primaryKeyColumns.contains(columnDefinition.name())) {
            columnType = relaxedType(columnType);
        }
        if (columnType.isBlank()) {
            return "";
        }
        return quoteIdentifier(columnDefinition.name()) + " " + columnType;
    }

    private static Set<String> primaryKeyColumns(List<String> definitions) {
        Set<String> columns = new LinkedHashSet<>();
        for (String definition : definitions) {
            String stripped = definition.strip();
            if (!stripped.toUpperCase(Locale.ROOT).startsWith("PRIMARY KEY")) {
                continue;
            }
            int openingParenthesisIndex = stripped.indexOf('(');
            int closingParenthesisIndex = openingParenthesisIndex < 0 ? -1 : matchingCloseParen(stripped, openingParenthesisIndex);
            if (openingParenthesisIndex < 0 || closingParenthesisIndex < 0) {
                continue;
            }
            for (String primaryKeyColumnDefinition : splitTopLevel(stripped.substring(openingParenthesisIndex + 1, closingParenthesisIndex))) {
                String primaryKeyColumnName = primaryKeyColumnDefinition.strip();
                if (primaryKeyColumnName.startsWith("`")) {
                    int closingBacktickIndex = primaryKeyColumnName.indexOf('`', 1);
                    if (closingBacktickIndex > 0) {
                        columns.add(primaryKeyColumnName.substring(1, closingBacktickIndex).replace("``", "`"));
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

    public static String relaxedType(String type) {
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

    private static int matchingCloseParen(String ddlFragment, int openingParenthesisIndex) {
        int parenthesisDepth = 0;
        char activeQuote = 0;
        for (int charIndex = openingParenthesisIndex; charIndex < ddlFragment.length(); charIndex++) {
            char currentChar = ddlFragment.charAt(charIndex);
            if (activeQuote != 0) {
                if (currentChar == activeQuote && (activeQuote == '`' || charIndex == 0 || ddlFragment.charAt(charIndex - 1) != '\\')) {
                    activeQuote = 0;
                }
                continue;
            }
            if (currentChar == '\'' || currentChar == '"' || currentChar == '`') {
                activeQuote = currentChar;
            } else if (currentChar == '(') {
                parenthesisDepth++;
            } else if (currentChar == ')') {
                parenthesisDepth--;
                if (parenthesisDepth == 0) {
                    return charIndex;
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
