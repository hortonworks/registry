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
package com.hortonworks.registries.storage;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.storage.exception.AlreadyExistsException;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class AbstractStoreManagerTest {
    protected static final Logger log = LoggerFactory.getLogger(AbstractStoreManagerTest.class);

    @Rule
    public TestName testName = new TestName();

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        public void starting(final Description method) {
            log.info("RUNNING TEST [{}] ", method.getMethodName());
        }
    };

    // To test a new Storable entity type, add it to this list in the implementation of the method setStorableTests
    protected List<StorableTest> storableTests;

    @Before
    public void setup() {
        setStorableTests();
        for (StorableTest test : storableTests) {
            test.setStorageManager(getStorageManager());
        }
    }

    // Method that sets the list of CRUD tests to be run
    protected abstract void setStorableTests();

    /**
     * @return When we add a new implementation for StorageManager interface we will also add a corresponding test implementation
     * which will extends this class and implement this method.
     * <p>
     * Essentially we are going to run the same test defined in this class for every single implementation of StorageManager.
     */
    protected abstract StorageManager getStorageManager();

    // ================ TEST METHODS ================
    // Test methods use the widely accepted naming convention  [UnitOfWork_StateUnderTest_ExpectedBehavior]

    @Test
    public void testCrud_AllStorableEntities_NoExceptions() {
        for (StorableTest test : storableTests) {
            try {
                test.init();
                test.test();
            } finally {
                test.close();
            }
        }
    }

    // UnequalExistingStorable => Storable that has the same StorableKey but does NOT verify .equals()
    @Test(expected = AlreadyExistsException.class)
    public void testAdd_UnequalExistingStorable_AlreadyExistsException() {
        for (StorableTest test : storableTests) {
            Storable storable1 = test.getStorableList().get(0);
            Storable storable2 = test.getStorableList().get(1);
            Assert.assertEquals(storable1.getStorableKey(), storable2.getStorableKey());
            Assert.assertNotEquals(storable1, storable2);
            getStorageManager().add(storable1);
            getStorageManager().add(storable2);     // should throw exception
        }
    }

    // EqualExistingStorable => Storable that has the same StorableKey and verifies .equals()
    @Test
    public void testAdd_EqualExistingStorable_NoOperation() {
        for (StorableTest test : storableTests) {
            Storable storable1 = test.getStorableList().get(0);
            getStorageManager().add(storable1);
            getStorageManager().addOrUpdate(storable1);
            Assert.assertEquals(storable1, getStorageManager().get(storable1.getStorableKey()));
        }
    }

    @Test
    public void testRemove_NonExistentStorable_null() {
        for (StorableTest test : storableTests) {
            Storable removed = getStorageManager().remove(test.getStorableList().get(0).getStorableKey());
            Assert.assertNull(removed);
        }
    }

    @Test(expected = StorageException.class)
    public void testList_NonexistentNameSpace_StorageException() {
        Assert.assertTrue(getStorageManager().list("NONEXISTENT_NAME_SPACE").isEmpty());
    }

    @Test
    public void testFind_NullQueryParams_AllEntries() {
        for (StorableTest test : storableTests) {
            test.addAllToStorage();
            Collection<Storable> allExisting = getStorageManager().list(test.getNameSpace());
            Collection<Storable> allMatchingQueryParamsFilter = getStorageManager().find(test.getNameSpace(), null);
            assertIterators(allExisting, allMatchingQueryParamsFilter);
        }
    }

    public void assertIterators(Collection collection1, Collection collection2) {
        Assert.assertTrue(Iterators.elementsEqual(collection1.iterator(), collection2.iterator()));
    }

    @Test
    public void testFindOrderBy() {
        for (StorableTest test : storableTests) {
            test.addAllToStorage();
            List<QueryParam> queryParams = Collections.emptyList();
            Collection<? extends Storable> allExisting = getStorageManager().list(test.getNameSpace());

            ArrayList<? extends Storable> sortedStorables = Lists.newArrayList(allExisting);
            sortedStorables.sort((Comparator<Storable>) (storable1, storable2) -> (int) (storable2.getId() - storable1.getId()));

            List<OrderByField> orderByFields = Lists.newArrayList(OrderByField.of("id", true));
            final Collection<Storable> allMatchingOrderByFilter = getStorageManager().find(test.getNameSpace(), queryParams, orderByFields);

            System.out.println("allMatchingOrderByFilter = " + allMatchingOrderByFilter);
            System.out.println("sortedStorables = " + sortedStorables);

            assertIterators(sortedStorables, allMatchingOrderByFilter);
        }
    }

    @Test
    public void testFind_NonExistentQueryParams_EmptyList() {
        for (StorableTest test : storableTests) {
            test.addAllToStorage();
            List<QueryParam> queryParams = new ArrayList<QueryParam>() {
                {
                    add(new QueryParam("NON_EXISTING_FIELD_1", "NON_EXISTING_VAL_1"));
                    add(new QueryParam("NON_EXISTING_FIELD_2", "NON_EXISTING_VAL_2"));
                }
            };

            final Collection<Storable> allMatchingQueryParamsFilter = getStorageManager().find(test.getNameSpace(), queryParams);
            assertIterators(Collections.EMPTY_LIST, allMatchingQueryParamsFilter);
        }
    }

    @Test
    public void testNextId_AutoincrementColumn_IdPlusOne() throws Exception {
        for (StorableTest test : storableTests) {
            // Device does not have auto_increment, and therefore there is no concept of nextId and should throw exception
            doTestNextId_AutoincrementColumn_IdPlusOne(test);
        }
    }

    protected void doTestNextId_AutoincrementColumn_IdPlusOne(StorableTest test) throws SQLException {
        Long actualNextId = getStorageManager().nextId(test.getNameSpace());
        Long expectedNextId = actualNextId;
        Assert.assertEquals(expectedNextId, actualNextId);
        addAndAssertNextId(test, 0, ++expectedNextId);
        addAndAssertNextId(test, 2, ++expectedNextId);
        addAndAssertNextId(test, 2, expectedNextId);
        addAndAssertNextId(test, 3, ++expectedNextId);
    }

    protected void addAndAssertNextId(StorableTest test, int idx, Long expectedId) throws SQLException {
        getStorageManager().addOrUpdate(test.getStorableList().get(idx));
        Long nextId = getStorageManager().nextId(test.getNameSpace());
        Assert.assertEquals(expectedId, nextId);
    }
}
