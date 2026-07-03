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
                BinlogPosition checkpoint = snapshotService.runIfNeeded();
                connect(checkpoint);
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

    private void connect(BinlogPosition checkpoint) throws Exception {
        JdbcUrlParser.MysqlEndpoint endpoint = checkpointService.sourceEndpoint();
        BinaryLogClient binaryLogClient = new BinaryLogClient(endpoint.host(), endpoint.port(),
                checkpointService.properties().getSource().getUsername(),
                checkpointService.properties().getSource().getPassword());
        binaryLogClient.setServerId(checkpointService.properties().getReplication().getServerId());
        if (checkpoint != null) {
            if (checkpoint.binlogFile() != null && checkpoint.binlogPosition() != null) {
                binaryLogClient.setBinlogFilename(checkpoint.binlogFile());
                binaryLogClient.setBinlogPosition(checkpoint.binlogPosition());
                log.info("Starting binlog client from checkpoint file={} position={}",
                        checkpoint.binlogFile(), checkpoint.binlogPosition());
            } else if (checkpointService.properties().getReplication().isUseGtid()
                    && checkpoint.gtidSet() != null && !checkpoint.gtidSet().isBlank()) {
                binaryLogClient.setGtidSet(checkpoint.gtidSet());
                log.info("Starting binlog client from checkpoint GTID set={}", checkpoint.gtidSet());
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
        private final List<RowChange> transaction = new ArrayList<>();
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
                EventData data = event.getData();
                if (data instanceof GtidEventData gtid) {
                    currentGtid = gtid.getGtid();
                } else if (data instanceof TableMapEventData tableMap) {
                    tablesById.put(tableMap.getTableId(), tableMap);
                } else if (checkpointService.properties().getReplication().isDmlEnabled()
                        && data instanceof WriteRowsEventData rows) {
                    collectRows(RowChange.Kind.INSERT, rows.getTableId(), null, rows.getRows(), event);
                } else if (checkpointService.properties().getReplication().isDmlEnabled()
                        && data instanceof UpdateRowsEventData rows) {
                    collectUpdates(rows, event);
                } else if (checkpointService.properties().getReplication().isDmlEnabled()
                        && data instanceof DeleteRowsEventData rows) {
                    collectRows(RowChange.Kind.DELETE, rows.getTableId(), rows.getRows(), null, event);
                } else if (isCommit(event)) {
                    commitTransaction(event);
                } else if (data instanceof QueryEventData query) {
                    handleQuery(event, query);
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
            String table = tableName(tableId);
            if (!tableFilter.accepts(table)) {
                return;
            }
            BinlogPosition position = position(event, kind.name(), table);
            if (kind == RowChange.Kind.INSERT) {
                for (Serializable[] row : afterRows) {
                    transaction.add(new RowChange(kind, table, null, row, position));
                }
            } else {
                for (Serializable[] row : beforeRows) {
                    transaction.add(new RowChange(kind, table, row, null, position));
                }
            }
        }

        private void collectUpdates(UpdateRowsEventData rows, Event event) {
            if (!isSourceTable(rows.getTableId())) {
                return;
            }
            String table = tableName(rows.getTableId());
            if (!tableFilter.accepts(table)) {
                return;
            }
            BinlogPosition position = position(event, "UPDATE", table);
            for (Map.Entry<Serializable[], Serializable[]> row : rows.getRows()) {
                transaction.add(new RowChange(RowChange.Kind.UPDATE, table, row.getKey(), row.getValue(), position));
            }
        }

        private void handleQuery(Event event, QueryEventData query) throws Exception {
            String sql = query.getSql();
            String normalized = sql.trim().toUpperCase(Locale.ROOT);
            if ("BEGIN".equals(normalized)) {
                transaction.clear();
                return;
            }
            if ("COMMIT".equals(normalized)) {
                commitTransaction(event);
                return;
            }
            if (!checkpointService.properties().getReplication().isDdlEnabled() || !isSupportedDdl(normalized)) {
                return;
            }
            if (query.getDatabase() != null && !query.getDatabase().isBlank()
                    && !query.getDatabase().equalsIgnoreCase(checkpointService.sourceEndpoint().database())) {
                return;
            }
            ddlApplier.apply(sql, position(event, "DDL", null));
        }

        private void commitTransaction(Event event) throws Exception {
            if (transaction.isEmpty()) {
                return;
            }
            BinlogPosition position = position(event, "COMMIT", lastTable());
            dmlApplier.applyTransaction(new ArrayList<>(transaction), position);
            transaction.clear();
        }

        private boolean isCommit(Event event) {
            return event.getHeader().getEventType() == EventType.XID;
        }

        private boolean isSupportedDdl(String sql) {
            return sql.startsWith("CREATE TABLE")
                    || sql.startsWith("ALTER TABLE")
                    || sql.startsWith("DROP TABLE")
                    || sql.startsWith("RENAME TABLE")
                    || sql.startsWith("TRUNCATE TABLE")
                    || sql.startsWith("CREATE INDEX")
                    || sql.startsWith("DROP INDEX");
        }

        private String tableName(long tableId) {
            TableMapEventData table = tablesById.get(tableId);
            if (table == null) {
                throw new IllegalStateException("Missing table map for tableId " + tableId);
            }
            return table.getTable();
        }

        private boolean isSourceTable(long tableId) {
            TableMapEventData table = tablesById.get(tableId);
            if (table == null) {
                throw new IllegalStateException("Missing table map for tableId " + tableId);
            }
            return table.getDatabase().equalsIgnoreCase(checkpointService.sourceEndpoint().database());
        }

        private String lastTable() {
            return transaction.isEmpty() ? null : transaction.get(transaction.size() - 1).table();
        }

        private BinlogPosition position(Event event, String eventType, String table) {
            return new BinlogPosition(client.getBinlogFilename(), nextPosition(event), currentGtid,
                    eventType, table, eventTime(event));
        }

        private Long nextPosition(Event event) {
            return ((EventHeaderV4) event.getHeader()).getNextPosition();
        }

        private LocalDateTime eventTime(Event event) {
            long millis = ((EventHeaderV4) event.getHeader()).getTimestamp();
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        }
    }
}
