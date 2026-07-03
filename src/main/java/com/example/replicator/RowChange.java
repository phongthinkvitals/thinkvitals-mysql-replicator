package com.example.replicator;

import java.io.Serializable;

record RowChange(
        Kind kind,
        String table,
        Serializable[] before,
        Serializable[] after,
        BinlogPosition position
) {
    enum Kind {
        INSERT, UPDATE, DELETE
    }
}
