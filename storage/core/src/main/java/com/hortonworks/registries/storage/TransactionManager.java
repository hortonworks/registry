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

package com.hortonworks.registries.storage;

import com.hortonworks.registries.common.transaction.TransactionIsolation;

public interface TransactionManager {

    /**
     * Begins the transaction
     */
    void beginTransaction(TransactionIsolation transactionIsolationLevel);


    /**
     * Discards the changes made to the storage layer and reverts to the last committed point
     */
    void rollbackTransaction();


    /**
     * Flushes the changes made to the storage layer
     */
    void commitTransaction();
}
