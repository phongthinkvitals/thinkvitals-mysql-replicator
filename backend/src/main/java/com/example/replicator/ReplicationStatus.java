package com.example.replicator;

import java.time.LocalDateTime;

public record ReplicationStatus(
        boolean running,
        boolean paused,
        String sourceDatabase,
        String sinkDatabase,
        String binlogFile,
        Long binlogPosition,
        String gtidSet,
        String lastTableName,
        String lastEventType,
        LocalDateTime lastEventTime,
        LocalDateTime lastAppliedTime,
        Long lagMs,
        boolean checkpointPresent,
        String checkpointSourceDatabase,
        String error
) {
}
