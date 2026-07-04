package com.example.replicator.service;

import com.example.replicator.model.BinlogPosition;
import com.example.replicator.model.ReplicationStatus;
import com.example.replicator.replication.DdlApplier;
import com.example.replicator.replication.DmlApplier;
import com.example.replicator.replication.RowChange;
import com.example.replicator.sql.TableFilter;
import com.example.replicator.util.JdbcUrlParser;
import com.example.replicator.util.Retryer;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ReplicationService implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(ReplicationService.class);

    private final SnapshotService snapshotService;
    private final DmlApplier dmlApplier;
    private final DdlApplier ddlApplier;
    private final CheckpointService checkpointService;
    private final TableFilter tableFilter;
    private final Retryer retryer;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private volatile BinaryLogClient client;
    private volatile Future<?> task;

    public ReplicationService(SnapshotService snapshotService,
                              DmlApplier dmlApplier,
                              DdlApplier ddlApplier,
                              CheckpointService checkpointService) {
        this.snapshotService = snapshotService;
        this.dmlApplier = dmlApplier;
        this.ddlApplier = ddlApplier;
        this.checkpointService = checkpointService;
        this.tableFilter = new TableFilter(checkpointService.properties().getReplication());
        this.retryer = new Retryer(checkpointService.properties().getRetry());
    }

    @Override
    public void run(ApplicationArguments args) {
        if (checkpointService.properties().getReplication().isAutoStart()) {
            resume();
            return;
        }
        paused.set(true);
        checkpointService.refreshStatus(false, true, null);
        log.info("Replication is waiting for an authorized run command");
    }

    public synchronized void pause() {
        paused.set(true);
        disconnect();
        checkpointService.refreshStatus(false, true, null);
    }

    public synchronized void resume() {
        if (running.get()) {
            paused.set(false);
            checkpointService.refreshStatus(true, false, null);
            return;
        }
        paused.set(false);
        task = executor.submit(this::runLoop);
    }

    public ReplicationStatus status() {
        checkpointService.refreshStatus(running.get(), paused.get(), checkpointService.status().error());
        return checkpointService.status();
    }

    @PreDestroy
    public void stop() {
        paused.set(true);
        disconnect();
        executor.shutdownNow();
    }

    private void runLoop() {
        running.set(true);
        checkpointService.refreshStatus(true, false, null);
        try {
            retryer.run("replication", log, () -> {
                BinlogPosition resumePosition = snapshotService.runIfNeeded();
                connect(resumePosition);
            });
        } catch (Exception e) {
            running.set(false);
            checkpointService.refreshStatus(false, paused.get(), e.getMessage());
            log.error("Replication failed error={}", e.getMessage(), e);
        } finally {
            running.set(false);
            if (!paused.get()) {
                checkpointService.refreshStatus(false, false, checkpointService.status().error());
            }
        }
    }

    private void connect(BinlogPosition resumePosition) throws Exception {
        JdbcUrlParser.MysqlEndpoint endpoint = checkpointService.sourceEndpoint();
        BinaryLogClient binaryLogClient = new BinaryLogClient(endpoint.host(), endpoint.port(),
                checkpointService.properties().getSource().getUsername(),
                checkpointService.properties().getSource().getPassword());
        binaryLogClient.setServerId(checkpointService.properties().getReplication().getServerId());
        if (resumePosition != null) {
            if (resumePosition.binlogFile() != null && resumePosition.binlogPosition() != null) {
                binaryLogClient.setBinlogFilename(resumePosition.binlogFile());
                binaryLogClient.setBinlogPosition(resumePosition.binlogPosition());
                log.info("Starting binlog client from checkpoint file={} position={}",
                        resumePosition.binlogFile(), resumePosition.binlogPosition());
            } else if (checkpointService.properties().getReplication().isUseGtid()
                    && resumePosition.gtidSet() != null && !resumePosition.gtidSet().isBlank()) {
                binaryLogClient.setGtidSet(resumePosition.gtidSet());
                log.info("Starting binlog client from checkpoint GTID set={}", resumePosition.gtidSet());
            }
        }
        BinlogEventHandler handler = new BinlogEventHandler(binaryLogClient);
        binaryLogClient.registerEventListener(handler::onEvent);
        this.client = binaryLogClient;
        binaryLogClient.connect();
    }

    private void disconnect() {
        BinaryLogClient current = client;
        if (current != null && current.isConnected()) {
            try {
                current.disconnect();
            } catch (Exception e) {
                log.warn("Failed to disconnect binlog client: {}", e.getMessage());
            }
        }
    }

    private class BinlogEventHandler {
        private final BinaryLogClient client;
        private final Map<Long, TableMapEventData> tablesById = new HashMap<>();
        private final List<RowChange> pendingTransactionChanges = new ArrayList<>();
        private String currentGtid;

        BinlogEventHandler(BinaryLogClient client) {
            this.client = client;
        }

        void onEvent(Event event) {
            try {
                if (paused.get()) {
                    disconnect();
                    return;
                }
                EventData eventData = event.getData();
                if (eventData instanceof GtidEventData gtidEventData) {
                    currentGtid = gtidEventData.getGtid();
                } else if (eventData instanceof TableMapEventData tableMap) {
                    tablesById.put(tableMap.getTableId(), tableMap);
                } else if (checkpointService.properties().getReplication().isDmlEnabled()
                        && eventData instanceof WriteRowsEventData writeRowsEventData) {
                    collectRows(RowChange.Kind.INSERT, writeRowsEventData.getTableId(), null, writeRowsEventData.getRows(), event);
                } else if (checkpointService.properties().getReplication().isDmlEnabled()
                        && eventData instanceof UpdateRowsEventData updateRowsEventData) {
                    collectUpdates(updateRowsEventData, event);
                } else if (checkpointService.properties().getReplication().isDmlEnabled()
                        && eventData instanceof DeleteRowsEventData deleteRowsEventData) {
                    collectRows(RowChange.Kind.DELETE, deleteRowsEventData.getTableId(), deleteRowsEventData.getRows(), null, event);
                } else if (isCommit(event)) {
                    commitTransaction(event);
                } else if (eventData instanceof QueryEventData queryEventData) {
                    handleQuery(event, queryEventData);
                }
            } catch (Exception e) {
                log.error("Replication failed eventType={} binlog={}:{} gtid={} error={}",
                        event.getHeader().getEventType(), client.getBinlogFilename(), nextPosition(event), currentGtid, e.getMessage(), e);
                disconnect();
                throw new RuntimeException(e);
            }
        }

        private void collectRows(RowChange.Kind kind, long tableId, List<Serializable[]> beforeRows,
                                 List<Serializable[]> afterRows, Event event) {
            if (!isSourceTable(tableId)) {
                return;
            }
            String tableName = tableName(tableId);
            if (!tableFilter.accepts(tableName)) {
                return;
            }
            BinlogPosition rowEventPosition = position(event, kind.name(), tableName);
            if (kind == RowChange.Kind.INSERT) {
                for (Serializable[] afterImage : afterRows) {
                    pendingTransactionChanges.add(new RowChange(kind, tableName, null, afterImage, rowEventPosition));
                }
            } else {
                for (Serializable[] beforeImage : beforeRows) {
                    pendingTransactionChanges.add(new RowChange(kind, tableName, beforeImage, null, rowEventPosition));
                }
            }
        }

        private void collectUpdates(UpdateRowsEventData updateRowsEventData, Event event) {
            if (!isSourceTable(updateRowsEventData.getTableId())) {
                return;
            }
            String tableName = tableName(updateRowsEventData.getTableId());
            if (!tableFilter.accepts(tableName)) {
                return;
            }
            BinlogPosition rowEventPosition = position(event, "UPDATE", tableName);
            for (Map.Entry<Serializable[], Serializable[]> rowImages : updateRowsEventData.getRows()) {
                pendingTransactionChanges.add(new RowChange(RowChange.Kind.UPDATE, tableName,
                        rowImages.getKey(), rowImages.getValue(), rowEventPosition));
            }
        }

        private void handleQuery(Event event, QueryEventData queryEventData) throws Exception {
            String querySql = queryEventData.getSql();
            String normalizedQuerySql = querySql.trim().toUpperCase(Locale.ROOT);
            if ("BEGIN".equals(normalizedQuerySql)) {
                pendingTransactionChanges.clear();
                return;
            }
            if ("COMMIT".equals(normalizedQuerySql)) {
                commitTransaction(event);
                return;
            }
            if (!checkpointService.properties().getReplication().isDdlEnabled() || !isSupportedDdl(normalizedQuerySql)) {
                return;
            }
            if (queryEventData.getDatabase() != null && !queryEventData.getDatabase().isBlank()
                    && !queryEventData.getDatabase().equalsIgnoreCase(checkpointService.sourceEndpoint().database())) {
                return;
            }
            ddlApplier.apply(querySql, position(event, "DDL", null));
        }

        private void commitTransaction(Event event) throws Exception {
            if (pendingTransactionChanges.isEmpty()) {
                return;
            }
            // Apply all row events atomically at XID/COMMIT and save the matching checkpoint.
            BinlogPosition commitPosition = position(event, "COMMIT", lastChangedTable());
            dmlApplier.applyTransaction(new ArrayList<>(pendingTransactionChanges), commitPosition);
            pendingTransactionChanges.clear();
        }

        private boolean isCommit(Event event) {
            return event.getHeader().getEventType() == EventType.XID;
        }

        private boolean isSupportedDdl(String normalizedQuerySql) {
            return normalizedQuerySql.startsWith("CREATE TABLE")
                    || normalizedQuerySql.startsWith("ALTER TABLE")
                    || normalizedQuerySql.startsWith("DROP TABLE")
                    || normalizedQuerySql.startsWith("RENAME TABLE")
                    || normalizedQuerySql.startsWith("TRUNCATE TABLE")
                    || normalizedQuerySql.startsWith("CREATE INDEX")
                    || normalizedQuerySql.startsWith("DROP INDEX");
        }

        private String tableName(long tableId) {
            TableMapEventData tableMap = tablesById.get(tableId);
            if (tableMap == null) {
                throw new IllegalStateException("Missing table map for tableId " + tableId);
            }
            return tableMap.getTable();
        }

        private boolean isSourceTable(long tableId) {
            TableMapEventData tableMap = tablesById.get(tableId);
            if (tableMap == null) {
                throw new IllegalStateException("Missing table map for tableId " + tableId);
            }
            return tableMap.getDatabase().equalsIgnoreCase(checkpointService.sourceEndpoint().database());
        }

        private String lastChangedTable() {
            return pendingTransactionChanges.isEmpty()
                    ? null
                    : pendingTransactionChanges.get(pendingTransactionChanges.size() - 1).table();
        }

        private BinlogPosition position(Event event, String eventType, String table) {
            return new BinlogPosition(client.getBinlogFilename(), nextPosition(event), currentGtid,
                    eventType, table, eventTime(event));
        }

        private Long nextPosition(Event event) {
            return ((EventHeaderV4) event.getHeader()).getNextPosition();
        }

        private LocalDateTime eventTime(Event event) {
            long eventTimestampMillis = ((EventHeaderV4) event.getHeader()).getTimestamp();
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(eventTimestampMillis), ZoneId.systemDefault());
        }
    }
}
