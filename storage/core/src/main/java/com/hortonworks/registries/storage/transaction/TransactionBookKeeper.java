/**
 * Copyright 2017 Hortonworks.
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

    private static final TransactionBookKeeper transactionBookKeeper = new TransactionBookKeeper();

    protected final ConcurrentHashMap<Long, TransactionContext> threadIdToConnectionMap = new ConcurrentHashMap<>();

    private TransactionBookKeeper() {

    }

    public boolean hasActiveTransaction(Long threadId) {
        return threadIdToConnectionMap.containsKey(threadId);
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
        TransactionContext transactionContext = threadIdToConnectionMap.get(threadId);
        transactionContext.incrementNestedTransactionCount();
        threadIdToConnectionMap.put(threadId, transactionContext);
    }

    public boolean removeTransaction(Long threadId) {
        if (threadIdToConnectionMap.containsKey(threadId)) {
            TransactionContext transactionContext = threadIdToConnectionMap.get(threadId);
            transactionContext.decrementNestedTransactionCount();
            if (transactionContext.getNestedTransactionCount() == 0) {
                threadIdToConnectionMap.remove(threadId);
                return true;
            } else
                threadIdToConnectionMap.put(threadId, transactionContext);
            return false;
        } else
            throw new TransactionException(String.format("No transaction is associated with thread id : %s", Long.toString(threadId)));
    }

    public static TransactionBookKeeper get() {
        return transactionBookKeeper;
    }
}
