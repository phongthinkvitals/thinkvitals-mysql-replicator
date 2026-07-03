package com.example.replicator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
class DdlApplier {
    private static final Logger log = LoggerFactory.getLogger(DdlApplier.class);
    private static final Pattern FIRST_QUOTED_NAME = Pattern.compile("`(?:[^`]+`\\.)?([^`]+)`");

    private final JdbcTemplate sink;
    private final CheckpointService checkpointService;
    private final MetadataService metadataService;
    private final String sourceDb;
    private final String sinkDb;

    DdlApplier(@Qualifier("sinkJdbcTemplate") JdbcTemplate sink,
               CheckpointService checkpointService,
               MetadataService metadataService) {
        this.sink = sink;
        this.checkpointService = checkpointService;
        this.metadataService = metadataService;
        this.sourceDb = checkpointService.sourceEndpoint().database();
        this.sinkDb = checkpointService.sinkEndpoint().database();
    }

    void apply(String sql, BinlogPosition position) {
        Optional<String> prepared = DdlSanitizer.prepare(sql, sourceDb, sinkDb,
                checkpointService.properties().getReplication().isStrictDdl());
        long start = System.currentTimeMillis();
        if (prepared.isEmpty()) {
            metadataService.invalidate(null);
            checkpointService.save(position);
            checkpointService.saveTableEvents(Collections.singletonList(tableName(sql)), position);
            log.warn("Skipped relaxed DDL sql=\"{}\" binlog={}:{} durationMs={}",
                    sql, position.binlogFile(), position.binlogPosition(), System.currentTimeMillis() - start);
            return;
        }
        String mapped = prepared.get();
        try {
            sink.execute(mapped);
            metadataService.invalidate(null);
            checkpointService.save(position);
            checkpointService.saveTableEvents(Collections.singletonList(tableName(sql)), position);
            log.info("Applied DDL sql=\"{}\" binlog={}:{} durationMs={}",
                    mapped, position.binlogFile(), position.binlogPosition(), System.currentTimeMillis() - start);
        } catch (RuntimeException e) {
            if (!isCreateTableAlreadyExists(sql, e)) {
                throw e;
            }
            metadataService.invalidate(null);
            checkpointService.save(position);
            checkpointService.saveTableEvents(Collections.singletonList(tableName(sql)), position);
            log.warn("Skipped CREATE TABLE because sink table already exists sql=\"{}\" binlog={}:{} durationMs={}",
                    mapped, position.binlogFile(), position.binlogPosition(), System.currentTimeMillis() - start);
        }
    }

    private boolean isCreateTableAlreadyExists(String sql, Throwable error) {
        if (!sql.trim().toUpperCase(Locale.ROOT).startsWith("CREATE TABLE")) {
            return false;
        }
        Throwable current = error;
        while (current != null) {
            if (current instanceof SQLException sqlException && sqlException.getErrorCode() == 1050) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String tableName(String sql) {
        Matcher matcher = FIRST_QUOTED_NAME.matcher(sql);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}
