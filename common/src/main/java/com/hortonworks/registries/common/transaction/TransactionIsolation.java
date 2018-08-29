package com.hortonworks.registries.common.transaction;

import java.sql.Connection;

public enum TransactionIsolation {

    // Depending upon the target database the transaction isolation is set.
    // For MYSQL and POSTGRES it is REPEATABLE_READ and
    // for Oracle it is SERIALIZABLE
    DATABASE_SENSITIVE(-2),
    // Set default isolation level as recommended by the JDBC driver.
    // When used inside a nested transaction retains current transaction isolation.
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
