/**
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.registries.schemaregistry.locks;

import com.hortonworks.registries.schemaregistry.SchemaLockStorable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.TransactionManager;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class WriteLock implements Lock {

    private String lockName;
    private Long lockHoldingThreadId = -1L;

    private TransactionManager transactionManager;

    @Override
    public String getName() {
        return lockName;
    }

    @Override
    public LockType getLockType() {
        return LockType.WRITE;
    }

    @Override
    public long getLockHoldingThreadId() {
        return lockHoldingThreadId;
    }

    @Override
    public boolean lock(Long time, TimeUnit timeUnit) {
        StorableKey storableKey = new SchemaLockStorable(lockName).getStorableKey();
        boolean isLocked = transactionManager.writeLock(storableKey, time, timeUnit);

        if (isLocked) {
            this.lockHoldingThreadId = Thread.currentThread().getId();
        } else {
            this.lockHoldingThreadId = -1L;
        }

        return isLocked;
    }

    public WriteLock(String lockName, TransactionManager transactionManager) {
        this.lockName = lockName;
        this.transactionManager = transactionManager;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WriteLock)) {
            return false;
        }
        WriteLock writeLock = (WriteLock) o;
        return Objects.equals(lockName, writeLock.lockName) &&
                Objects.equals(lockHoldingThreadId, writeLock.lockHoldingThreadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lockName, getLockHoldingThreadId());
    }

    @Override
    public String toString() {
        return "WriteLock{" +
                "lockName='" + lockName + '\'' +
                ", lockHoldingThreadId=" + lockHoldingThreadId +
                '}';
    }
}
