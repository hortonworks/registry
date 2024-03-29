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
package com.hortonworks.registries.storage.impl.memory;

import com.hortonworks.registries.storage.AbstractStoreManagerTest;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableTest;
import com.hortonworks.registries.storage.StorageManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collection;

public abstract class AbstractInMemoryStorageManagerTest extends AbstractStoreManagerTest {
    private final StorageManager storageManager = new InMemoryStorageManager();

    @Override
    public StorageManager getStorageManager() {
        return storageManager;
    }

    @Test
    public void testListNonexistentNameSpaceStorageException() {
        Collection<Storable> found = getStorageManager().list("NONEXISTENT_NAME_SPACE");
        Assertions.assertTrue(found.isEmpty());
    }

    protected void doTestNextIdAutoincrementColumnIdPlusOne(StorableTest test) throws SQLException {
        Long actualNextId = getStorageManager().nextId(test.getNameSpace());
        Long expectedNextId = actualNextId;
        // increment two numbers for InmemoryManager as nextId() always increments.
        expectedNextId += 2;
        addAndAssertNextId(test, 0, expectedNextId);
        expectedNextId += 2;
        addAndAssertNextId(test, 2, expectedNextId);
        addAndAssertNextId(test, 2, ++expectedNextId);
        expectedNextId += 2;
        addAndAssertNextId(test, 3, expectedNextId);
    }

}
