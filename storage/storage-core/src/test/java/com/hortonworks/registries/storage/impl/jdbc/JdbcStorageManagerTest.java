/*
  Copyright 2016-2021 Cloudera, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package com.hortonworks.registries.storage.impl.jdbc;

import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OffsetProperties;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableFactory;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.storage.StorageProviderProperties;
import com.hortonworks.registries.storage.catalog.AbstractStorable;
import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.exception.DatabaseLockException;
import com.hortonworks.registries.storage.exception.OffsetRangeReachedException;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.impl.jdbc.provider.QueryExecutorFactory;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.sequences.NamespaceSequenceStorable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hortonworks.registries.common.Schema.Field.of;
import static com.hortonworks.registries.common.Schema.Type.LONG;
import static com.hortonworks.registries.common.Schema.Type.STRING;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcStorageManagerTest {

    private static final String NAMESPACE = "test-namespace";

    private final StorableFactory storableFactory = mock(StorableFactory.class);
    private final QueryExecutor queryExecutor = mock(QueryExecutor.class);
    private final JdbcStorageManager jdbcStorageManager = new JdbcStorageManager(queryExecutor, storableFactory);

    @Nested
    @DisplayName("Sequence generation when no offset is configured")
    class SequenceGenerationNoOffset {

        @Test
        public void stringFieldPKShouldBeIgnored() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());

            jdbcStorageManager.registerStorables(singleton(StringIdStorable.class));

            verify(queryExecutor, never()).insert(any());
        }

        @Test
        public void storablesWithCompoundPKsShouldBeIgnored() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());

            jdbcStorageManager.registerStorables(singleton(CompoundIdStorable.class));

            verify(queryExecutor, never()).insert(any());
        }

        @Test
        public void existingSequenceIsQueriedAndNotInitialized() {
            NamespaceSequenceStorable sequenceStorable = new NamespaceSequenceStorable(NAMESPACE, 5L);

            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequenceStorable));

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(queryExecutor).select(eq(sequenceStorable.getStorableKey()));
            verify(queryExecutor, never()).insert(any());
        }

        @Test
        public void afterFailedInsertWhileInitializingDbValueIsQueriedAgainToSeeIfAlreadyInitializedNow() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());

            AtomicBoolean first = new AtomicBoolean(true);
            when(queryExecutor.select(any(StorableKey.class))).thenAnswer((Answer<Collection<NamespaceSequenceStorable>>) invocation -> {
                        boolean currentVal = first.get();
                        first.set(false);
                        return currentVal ? emptyList() : singletonList(new NamespaceSequenceStorable(NAMESPACE, 1L));
                    }
            );

            doThrow(new StorageException("wasn't there first time but now it is!"))
                    .when(queryExecutor).insert(any(Storable.class));

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            PrimaryKey primaryKey = new PrimaryKey(singletonMap(NamespaceSequenceStorable.NAMESPACE_FIELD, NAMESPACE));
            verify(queryExecutor, times(2))
                    .select(new StorableKey(NamespaceSequenceStorable.NAMESPACE, primaryKey));
        }

        @Test
        public void afterFailedInsertDbValueQueriedAgainAndExpectedToBeAlreadyInitialized() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());
            doThrow(new StorageException("wasn't there first but now it is!"))
                    .when(queryExecutor).insert(any(Storable.class));

            assertThrows(
                    StorageException.class,
                    () -> jdbcStorageManager.registerStorables(singleton(LongIdStorable.class))
            );

            PrimaryKey primaryKey = new PrimaryKey(singletonMap(NamespaceSequenceStorable.NAMESPACE_FIELD, NAMESPACE));
            verify(queryExecutor, times(2))
                    .select(new StorableKey(NamespaceSequenceStorable.NAMESPACE, primaryKey));
        }

        @Test
        public void whenInitializingMaxIdIsSelected() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());
            when(queryExecutor.selectAggregate(any(), any(), any())).thenReturn(Optional.of(2L));

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(queryExecutor).selectAggregate(NAMESPACE, LongIdStorable.field, "MAX");
        }

        @Test
        public void sequenceForEmptyNamespaceShouldBeInitializedTo1() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 1L)));
        }

        @Test
        public void sequenceShouldBeInitializedWithMaxPlus1() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());
            when(queryExecutor.selectAggregate(any(), any(), any())).thenReturn(Optional.of(2L));

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 3L)));
        }

        @Test
        public void sequenceIsNotInitializedForAutoincrementStorables() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());

            jdbcStorageManager.registerStorables(singleton(AutoIncementStorable.class));

            verify(queryExecutor, never()).select(any(StorableKey.class));
            verify(queryExecutor, never()).insert(any());
        }

        @Nested
        @DisplayName("Sequence generation with offsetting")
        class SequenceOffsetting {

            private final long offsetMin = 1000L;
            private final long offsetMax = 2000L;

            StorageProviderConfiguration storageConfig = new StorageProviderConfiguration();

            {
                StorageProviderProperties providerProperties = new StorageProviderProperties();
                storageConfig.setProperties(providerProperties);
                providerProperties.setDbtype("mysql");
                OffsetProperties offsetProperties = new OffsetProperties();
                providerProperties.setOffsetRange(offsetProperties);
                offsetProperties.setMin(offsetMin);
                offsetProperties.setMax(offsetMax);
            }

            @Test
            public void newSequenceShouldBeInitializedToMaxIdWhenAboveTheOffset() {
                doNothing().when(storableFactory).addStorableClasses(anyCollection());

                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());
                    when(queryExecutor.selectAggregate(any(), any(), any())).thenReturn(Optional.of(1001L));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 1002L)));
                }
            }

            @Test
            public void newSequenceShouldBeOffsetedWhenMaxIdIsBelowTheOffset() {
                doNothing().when(storableFactory).addStorableClasses(anyCollection());

                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());
                    when(queryExecutor.selectAggregate(any(), any(), any())).thenReturn(Optional.of(1L));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 1000L)));
                }
            }

            @Test
            public void existingSequenceShouldBeIncreasedToTheOffsetWhenBelowOffset() {
                doNothing().when(storableFactory).addStorableClasses(anyCollection());

                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    NamespaceSequenceStorable sequenceStorable = new NamespaceSequenceStorable(NAMESPACE, 5L);
                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequenceStorable));
                    when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequenceStorable));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    verify(queryExecutor).update(eq(new NamespaceSequenceStorable(NAMESPACE, offsetMin)));
                }
            }

            @Test
            public void sequnceUpdateisSkippedWhenStorableCannotBeLocked() {
                doNothing().when(storableFactory).addStorableClasses(anyCollection());

                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    NamespaceSequenceStorable sequenceStorable = new NamespaceSequenceStorable(NAMESPACE, 5L);
                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequenceStorable));
                    when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(emptyList());

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    verify(queryExecutor, never()).update(eq(new NamespaceSequenceStorable(NAMESPACE, offsetMin)));
                }
            }

            @Test
            public void existingSequenceShouldBeUnchangedWhenAboveTheOffset() {
                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    NamespaceSequenceStorable sequenceStorable = new NamespaceSequenceStorable(NAMESPACE, 1005L);
                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequenceStorable));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    verify(queryExecutor, never()).insert(any(Storable.class));
                    verify(queryExecutor, never()).update(any(Storable.class));
                }
            }

            @Test
            public void maxOffsetIsStillAllowed() {
                LongIdStorable storable = new LongIdStorable();
                String nameSpace = storable.getNameSpace();
                doNothing().when(storableFactory).addStorableClasses(anyCollection());
                when(storableFactory.create(anyString())).thenReturn(storable);

                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(nameSpace, offsetMax);
                    when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    assertEquals(offsetMax, jdbcStorageManager.nextId(nameSpace));

                    verify(queryExecutor).update(eq(new NamespaceSequenceStorable(nameSpace, offsetMax + 1)));
                }
            }

            @Test
            public void goingAboveTheMaxOffsetIsNotAllowed() {
                LongIdStorable storable = new LongIdStorable();
                String namespace = storable.getNameSpace();
                doNothing().when(storableFactory).addStorableClasses(anyCollection());
                when(storableFactory.create(anyString())).thenReturn(storable);

                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(namespace, offsetMax + 1);
                    when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    assertThrows(OffsetRangeReachedException.class, () -> jdbcStorageManager.nextId(namespace));

                    verify(queryExecutor, never()).update(any());
                }
            }
        }

        @Nested
        @DisplayName("Next id sequence generation")
        class NextId {

            @Test
            public void unsuccessfulLockingExistingSequenceThrowsException() {
                LongIdStorable storable = new LongIdStorable();
                String namespace = storable.getNameSpace();
                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(namespace, 0L);

                when(storableFactory.create(anyString())).thenReturn(storable);
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(emptyList());
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                assertThrows(DatabaseLockException.class, () -> jdbcStorageManager.nextId(namespace));

                verify(queryExecutor, atLeastOnce()).selectForUpdate(eq(sequence.getStorableKey()));
            }

            @Test
            public void unsuccessfulLockingBecauseSequenceDoesNotExistInitializesSequence() {
                LongIdStorable storable = new LongIdStorable();
                String namespace = storable.getNameSpace();
                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(namespace, 0L);

                when(storableFactory.create(anyString())).thenReturn(storable);
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(emptyList());
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

                jdbcStorageManager.nextId(namespace);

                verify(queryExecutor).select(eq(sequence.getStorableKey()));
                verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(namespace, 2L)));
            }

            @Test
            public void throwsWhenCannotGetSequenceAfterSuccessfulLocking() {
                LongIdStorable storable = new LongIdStorable();
                String namespace = storable.getNameSpace();
                when(storableFactory.create(anyString())).thenReturn(storable);

                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(namespace, 5L);
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

                assertThrows(DatabaseLockException.class, () -> jdbcStorageManager.nextId(namespace));

                verify(queryExecutor).select(eq(sequence.getStorableKey()));
            }

            @Test
            public void incrementsSequenceAfterSuccessfulLocking() {
                when(storableFactory.create(anyString())).thenReturn(new LongIdStorable());
                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(NAMESPACE, 5L);
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                jdbcStorageManager.nextId(NAMESPACE);

                verify(queryExecutor).select(eq(sequence.getStorableKey()));
                verify(queryExecutor).update(eq(new NamespaceSequenceStorable(NAMESPACE, 6L)));
            }

            @Test
            public void autoIncrementStoragesDeferToQueryExecutorWhenGeneratingNextId() {
                AutoIncementStorable autoIncementStorable = new AutoIncementStorable();
                when(storableFactory.create(anyString())).thenReturn(autoIncementStorable);
                String nameSpace = autoIncementStorable.getNameSpace();

                jdbcStorageManager.nextId(nameSpace);

                verify(queryExecutor).nextId(eq(nameSpace));
            }
        }
    }

    static class StringIdStorable extends TestStorable {
        protected StringIdStorable() {
            super(of("stringField", STRING));
        }
    }

    static class LongIdStorable extends TestStorable {
        static Schema.Field field = of("longField", LONG);

        protected LongIdStorable() {
            super(field);
        }
    }

    static class AutoIncementStorable extends TestStorable {
        static Schema.Field field = of("longField", LONG);

        protected AutoIncementStorable() {
            super(field);
        }

        @Override
        public boolean isIdAutoIncremented() {
            return true;
        }
    }

    static class CompoundIdStorable extends AbstractStorable {
        private final Schema.Field field1 = of("longField1", LONG);
        private final Schema.Field field2 = of("longField2", LONG);

        protected CompoundIdStorable() {
        }

        @Override
        public String getNameSpace() {
            return NAMESPACE;
        }

        @Override
        public Schema getSchema() {
            return Schema.of(field1, field2);
        }

        @Override
        public PrimaryKey getPrimaryKey() {
            return new PrimaryKey(ImmutableMap.of(field1, 1L, field2, 2L));
        }
    }

    static class TestStorable extends AbstractStorable {

        private Schema.Field field;

        private TestStorable() {
        }

        protected TestStorable(Schema.Field soleField) {
            this.field = soleField;
        }

        @Override
        public String getNameSpace() {
            return NAMESPACE;
        }

        @Override
        public Schema getSchema() {
            return Schema.of(field);
        }

        @Override
        public PrimaryKey getPrimaryKey() {
            return new PrimaryKey(singletonMap(field, null));
        }

        @Override
        public Map<String, Object> toMap() {
            return singletonMap(field.getName(), null);
        }

        @Override
        public boolean isIdAutoIncremented() {
            return false;
        }
    }
}

