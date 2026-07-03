package com.example.replicator;

import java.time.LocalDateTime;

record BinlogPosition(
        String binlogFile,
        Long binlogPosition,
        String gtidSet,
        String lastEventType,
        String lastTableName,
        LocalDateTime lastEventTime
) {
}
