/**
 * Copyright 2017-2019 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.registries.storage.transaction;

import com.hortonworks.registries.storage.exception.TransactionException;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionBookKeeper {

    protected final ConcurrentHashMap<Long, TransactionContext> threadIdToConnectionMap = new ConcurrentHashMap<>();

    public boolean hasActiveTransaction(Long threadId) {
        return threadIdToConnectionMap.containsKey(threadId) && threadIdToConnectionMap.get(threadId).getNestedTransactionCount() != 0;
    }

    public Connection getConnection(Long threadId) {
        return threadIdToConnectionMap.get(threadId).getConnection();
    }

    public void addTransaction(Long threadId, Connection connection) {
        if (!threadIdToConnectionMap.containsKey(threadId)) {
            threadIdToConnectionMap.put(threadId, new TransactionContext(connection));
        } else {
            throw new TransactionException(String.format("A transaction is already associated with thread id : %s", Long.toString(threadId)));
        }
    }

    public void incrementNestedTransactionCount(Long threadId) {
        if (threadIdToConnectionMap.containsKey(threadId)) {
            TransactionContext transactionContext = threadIdToConnectionMap.get(threadId);
            transactionContext.incrementNestedTransactionCount();
            threadIdToConnectionMap.put(threadId, transactionContext);
        } else {
            throw new TransactionException(String.format("No transaction is associated with thread id : %s", Long.toString(threadId)));
        }
    }

    public void decrementNestedTransactionCount(Long threadId, TransactionState transactionState) {
        if (threadIdToConnectionMap.containsKey(threadId)) {
            TransactionContext transactionContext = threadIdToConnectionMap.get(threadId);
            transactionContext.decrementNestedTransactionCount();
            if (transactionContext.getNestedTransactionCount() < 0) {
                throw new TransactionException("Transaction was rolledback/committed more than necessary");
            }
            transactionContext.recordState(transactionState);
        } else {
            throw new TransactionException(String.format("No transaction is associated with thread id : %s", Long.toString(threadId)));
        }
    }

    public boolean whereThereAnyRollbacks(Long threadId) {
        return (threadIdToConnectionMap.get(threadId).getTransactionState() & TransactionState.ROLLBACK.value) == TransactionState.ROLLBACK.value;
    }

    public boolean whereThereAnyCommits(Long threadId) {
        return (threadIdToConnectionMap.get(threadId).getTransactionState() & TransactionState.COMMIT.value) == TransactionState.COMMIT.value;
    }

    public void removeTransaction(Long threadId) {
        if (threadIdToConnectionMap.containsKey(threadId)) {
            threadIdToConnectionMap.remove(threadId);
        } else {
            throw new TransactionException(String.format("No transaction is associated with thread id : %s", Long.toString(threadId)));
        }
    }
}
