package com.hortonworks.registries.common.transaction;

import java.sql.Connection;

public enum TransactionIsolation {
    DEFAULT(-1),
    // Commenting out below isolation level as Oracle doesn't support it.
    //READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    // Commenting out below isolation level as Oracle doesn't support it.
    // REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    int value;

    TransactionIsolation(int isolationValue) {
        this.value = isolationValue;
    }

    public int getValue() {
        return value;
    }
}
