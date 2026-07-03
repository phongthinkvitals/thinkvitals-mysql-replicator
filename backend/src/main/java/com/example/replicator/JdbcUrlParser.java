package com.example.replicator;

import java.net.URI;

final class JdbcUrlParser {
    private JdbcUrlParser() {
    }

    static MysqlEndpoint parse(String jdbcUrl, String configuredDatabase) {
        String raw = jdbcUrl.substring("jdbc:".length());
        URI uri = URI.create(raw.substring(0, raw.indexOf('?') > -1 ? raw.indexOf('?') : raw.length()));
        String database = configuredDatabase != null && !configuredDatabase.isBlank()
                ? configuredDatabase
                : uri.getPath().replaceFirst("^/", "");
        return new MysqlEndpoint(uri.getHost(), uri.getPort() == -1 ? 3306 : uri.getPort(), database);
    }

    record MysqlEndpoint(String host, int port, String database) {
    }
}
