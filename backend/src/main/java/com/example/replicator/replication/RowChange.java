package com.example.replicator.replication;

import com.example.replicator.model.BinlogPosition;

import java.io.Serializable;

public record RowChange(
        Kind kind,
        String table,
        Serializable[] before,
        Serializable[] after,
        BinlogPosition position
) {
    public enum Kind {
        INSERT, UPDATE, DELETE
    }
}
