package com.example.replicator.replication;

import com.example.replicator.metadata.MetadataService;
import com.example.replicator.model.BinlogPosition;
import com.example.replicator.schema.DdlSanitizer;
import com.example.replicator.service.CheckpointService;
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
public class DdlApplier {
    private static final Logger log = LoggerFactory.getLogger(DdlApplier.class);
    private static final Pattern FIRST_QUOTED_NAME = Pattern.compile("`(?:[^`]+`\\.)?([^`]+)`");

    private final JdbcTemplate sinkJdbcTemplate;
    private final CheckpointService checkpointService;
    private final MetadataService metadataService;
    private final String sourceDatabaseName;
    private final String sinkDatabaseName;

    public DdlApplier(@Qualifier("sinkJdbcTemplate") JdbcTemplate sinkJdbcTemplate,
               CheckpointService checkpointService,
               MetadataService metadataService) {
        this.sinkJdbcTemplate = sinkJdbcTemplate;
        this.checkpointService = checkpointService;
        this.metadataService = metadataService;
        this.sourceDatabaseName = checkpointService.sourceEndpoint().database();
        this.sinkDatabaseName = checkpointService.sinkEndpoint().database();
    }

    public void apply(String sourceDdlSql, BinlogPosition ddlPosition) {
        Optional<String> sanitizedDdlSql = DdlSanitizer.prepare(sourceDdlSql, sourceDatabaseName, sinkDatabaseName,
                checkpointService.properties().getReplication().isStrictDdl());
        long startTimeMillis = System.currentTimeMillis();
        if (sanitizedDdlSql.isEmpty()) {
            // Relaxed mode intentionally skips constraint/index-only DDL to keep CDC writable.
            metadataService.invalidate(null);
            checkpointService.save(ddlPosition);
            checkpointService.saveTableEvents(Collections.singletonList(tableName(sourceDdlSql)), ddlPosition);
            log.warn("Skipped relaxed DDL sql=\"{}\" binlog={}:{} durationMs={}",
                    sourceDdlSql, ddlPosition.binlogFile(), ddlPosition.binlogPosition(), System.currentTimeMillis() - startTimeMillis);
            return;
        }
        String sinkDdlSql = sanitizedDdlSql.get();
        try {
            sinkJdbcTemplate.execute(sinkDdlSql);
            metadataService.invalidate(null);
            checkpointService.save(ddlPosition);
            checkpointService.saveTableEvents(Collections.singletonList(tableName(sourceDdlSql)), ddlPosition);
            log.info("Applied DDL sql=\"{}\" binlog={}:{} durationMs={}",
                    sinkDdlSql, ddlPosition.binlogFile(), ddlPosition.binlogPosition(), System.currentTimeMillis() - startTimeMillis);
        } catch (RuntimeException e) {
            if (!isCreateTableAlreadyExists(sourceDdlSql, e)) {
                throw e;
            }
            metadataService.invalidate(null);
            checkpointService.save(ddlPosition);
            checkpointService.saveTableEvents(Collections.singletonList(tableName(sourceDdlSql)), ddlPosition);
            log.warn("Skipped CREATE TABLE because sinkJdbcTemplate table already exists sql=\"{}\" binlog={}:{} durationMs={}",
                    sinkDdlSql, ddlPosition.binlogFile(), ddlPosition.binlogPosition(), System.currentTimeMillis() - startTimeMillis);
        }
    }

    private boolean isCreateTableAlreadyExists(String sourceDdlSql, Throwable error) {
        if (!sourceDdlSql.trim().toUpperCase(Locale.ROOT).startsWith("CREATE TABLE")) {
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

    private String tableName(String ddlSql) {
        Matcher quotedTableNameMatcher = FIRST_QUOTED_NAME.matcher(ddlSql);
        if (quotedTableNameMatcher.find()) {
            return quotedTableNameMatcher.group(1);
        }
        return null;
    }
}
