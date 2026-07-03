package com.example.replicator.model;

import java.time.LocalDateTime;

public record BinlogPosition(
        String binlogFile,
        Long binlogPosition,
        String gtidSet,
        String lastEventType,
        String lastTableName,
        LocalDateTime lastEventTime,
        String sourceDatabase
) {
    public BinlogPosition(String binlogFile,
                          Long binlogPosition,
                          String gtidSet,
                          String lastEventType,
                          String lastTableName,
                          LocalDateTime lastEventTime) {
        this(binlogFile, binlogPosition, gtidSet, lastEventType, lastTableName, lastEventTime, null);
    }
}
