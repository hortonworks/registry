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
import com.hortonworks.registries.storage.impl.jdbc.provider.QueryExecutorFactory;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.sequences.NamespaceSequenceStorable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.hortonworks.registries.common.Schema.Field.of;
import static com.hortonworks.registries.common.Schema.Type.LONG;
import static com.hortonworks.registries.common.Schema.Type.STRING;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
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
        public void whenInitializingMaxIdIsSelected() throws SQLException {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

            Connection connection = mockSelectMaxQuery(null);

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(connection).prepareStatement(eq(format("SELECT MAX(%s) FROM %s", LongIdStorable.field.getName(), NAMESPACE)));
        }

        @Test
        public void sequenceForEmptyNamespaceShouldBeInitializedTo1() throws SQLException {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

            mockSelectMaxQuery(null);

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 1L)));
        }

        @Test
        public void sequenceShouldBeInitializedWithMaxPlus1() throws SQLException {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

            mockSelectMaxQuery(2L);

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 3L)));
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
            public void newSequenceShouldBeInitializedToMaxIdWhenAboveTheOffset() throws SQLException {
                doNothing().when(storableFactory).addStorableClasses(anyCollection());
                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

                    mockSelectMaxQuery(1001L);

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 1002L)));
                }
            }

            @Test
            public void newSequenceShouldBeOffsetedWhenMaxIdIsBelowTheOffset() throws SQLException {
                doNothing().when(storableFactory).addStorableClasses(anyCollection());
                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

                    mockSelectMaxQuery(1L);

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

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    verify(queryExecutor).update(eq(new NamespaceSequenceStorable(NAMESPACE, offsetMin)));
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
                doNothing().when(storableFactory).addStorableClasses(anyCollection());
                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(NAMESPACE, offsetMax);
                    when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    assertEquals(offsetMax, jdbcStorageManager.nextId(NAMESPACE));

                    verify(queryExecutor).update(eq(new NamespaceSequenceStorable(NAMESPACE, offsetMax + 1)));
                }
            }

            @Test
            public void goingAboveTheMaxOffsetIsNotAllowed() {
                doNothing().when(storableFactory).addStorableClasses(anyCollection());
                try (MockedStatic<QueryExecutorFactory> queryExecutorFactory = mockStatic(QueryExecutorFactory.class)) {
                    queryExecutorFactory
                            .when(() -> QueryExecutorFactory.get(any(DatabaseType.class), any(StorageProviderConfiguration.class)))
                            .thenReturn(queryExecutor);

                    NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(NAMESPACE, offsetMax + 1);
                    when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                    when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                    jdbcStorageManager.init(storageConfig);
                    jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

                    assertThrows(IllegalStateException.class, () -> jdbcStorageManager.nextId(NAMESPACE));

                    verify(queryExecutor, never()).update(any());
                }
            }
        }

        @Nested
        @DisplayName("Next id sequence generation")
        class NextId {

            @Test
            public void unsuccessfulLockingExistingSequenceThrowsException() {
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(emptyList());
                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(NAMESPACE, 0L);
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                assertThrows(IllegalStateException.class, () -> jdbcStorageManager.nextId(NAMESPACE));

                verify(queryExecutor, atLeastOnce()).selectForUpdate(eq(sequence.getStorableKey()));
            }

            @Test
            public void unsuccessfulLockingBecauseSequenceDoesNotExistInitializesSequence() {
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(emptyList());
                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(NAMESPACE, 0L);
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

                jdbcStorageManager.nextId(NAMESPACE);

                verify(queryExecutor).select(eq(sequence.getStorableKey()));
                verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 2L)));
            }

            @Test
            public void throwsWhenCannotGetSequenceAfterSuccessfulLocking() {
                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(NAMESPACE, 5L);
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());

                assertThrows(IllegalStateException.class, () -> jdbcStorageManager.nextId(NAMESPACE));

                verify(queryExecutor).select(eq(sequence.getStorableKey()));
            }

            @Test
            public void incrementsSequenceAfterSuccessfulLocking() {
                NamespaceSequenceStorable sequence = new NamespaceSequenceStorable(NAMESPACE, 5L);
                when(queryExecutor.selectForUpdate(any(StorableKey.class))).thenReturn(singleton(sequence));
                when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(sequence));

                jdbcStorageManager.nextId(NAMESPACE);

                verify(queryExecutor).select(eq(sequence.getStorableKey()));
                verify(queryExecutor).update(eq(new NamespaceSequenceStorable(NAMESPACE, 6L)));
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
    }

    private Connection mockSelectMaxQuery(Long maxValue) throws SQLException {
        boolean emptyTable = maxValue == null;
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(queryExecutor.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(!emptyTable);
        if (maxValue != null) {
            when(resultSet.getLong(eq(1))).thenReturn(maxValue);
        }
        return connection;
    }
}

