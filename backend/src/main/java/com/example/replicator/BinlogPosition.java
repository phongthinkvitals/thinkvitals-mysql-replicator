package com.example.replicator;

import java.time.LocalDateTime;

record BinlogPosition(
        String binlogFile,
        Long binlogPosition,
        String gtidSet,
        String lastEventType,
        String lastTableName,
        LocalDateTime lastEventTime,
        String sourceDatabase
) {
    BinlogPosition(String binlogFile,
                   Long binlogPosition,
                   String gtidSet,
                   String lastEventType,
                   String lastTableName,
                   LocalDateTime lastEventTime) {
        this(binlogFile, binlogPosition, gtidSet, lastEventType, lastTableName, lastEventTime, null);
    }
}
