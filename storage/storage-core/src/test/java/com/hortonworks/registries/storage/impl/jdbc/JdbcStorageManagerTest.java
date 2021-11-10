/*
  Copyright 2016-2019 Cloudera, Inc.

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

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.StorableFactory;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.sequences.NamespaceSequenceStorable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.hortonworks.registries.common.Schema.Type.LONG;
import static com.hortonworks.registries.common.Schema.Type.STRING;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcStorageManagerTest {

    private static String NAMESPACE = "test-namespace";

    private StorableFactory storableFactory = mock(StorableFactory.class);
    private QueryExecutor queryExecutor = mock(QueryExecutor.class);
    private JdbcStorageManager jdbcStorageManager = new JdbcStorageManager(queryExecutor, storableFactory);

    @Nested
    @DisplayName("Sequence generation")
    class SequenceGeneration {

        @Test
        public void stringFieldPKShouldBeIgnored() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());

            jdbcStorageManager.registerStorables(singleton(StringIdStorable.class));

            verify(queryExecutor, never()).insert(any());
        }

        @Test
        public void existingSequenceAreNotInitialized() {
            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(singleton(new NamespaceSequenceStorable()));

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(queryExecutor, never()).insert(any());
        }

        @Test
        public void whenInitializingANewSeqMaxIdShouldBeSelected() throws SQLException {
            Connection connection = mock(Connection.class);
            PreparedStatement preparedStatement = mock(PreparedStatement.class);
            ResultSet resultSet = mock(ResultSet.class);

            doNothing().when(storableFactory).addStorableClasses(anyCollection());
            when(queryExecutor.select(any(StorableKey.class))).thenReturn(emptyList());
            when(queryExecutor.getConnection()).thenReturn(connection);
            when(connection.prepareStatement(any())).thenReturn(preparedStatement);
            when(preparedStatement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getLong(eq(1))).thenReturn(2L);

            jdbcStorageManager.registerStorables(singleton(LongIdStorable.class));

            verify(connection).prepareStatement(eq(format("SELECT MAX(%s) FROM %s", LongIdStorable.field.getName(), NAMESPACE)));
            verify(queryExecutor).insert(eq(new NamespaceSequenceStorable(NAMESPACE, 3L)));
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

            Assertions.assertThrows(IllegalStateException.class, () -> jdbcStorageManager.nextId(NAMESPACE));

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

            Assertions.assertThrows(IllegalStateException.class, () -> jdbcStorageManager.nextId(NAMESPACE));

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

    static class StringIdStorable extends TestStorable {
        protected StringIdStorable() {
            super(Schema.Field.of("stringField", STRING));
        }
    }

    static class LongIdStorable extends TestStorable {
        static Schema.Field field = Schema.Field.of("longField", LONG);

        protected LongIdStorable() {
            super(field);
        }
    }

    static class TestStorable extends AbstractStorable {

        private Schema.Field field;

        protected TestStorable() {
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
}
