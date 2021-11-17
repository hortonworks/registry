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

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.AbstractStorableSqlQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.DefaultStorageDataTypeContext;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.hortonworks.registries.storage.impl.jdbc.util.SchemaFields.getSequenceField;
import static com.hortonworks.registries.storage.impl.jdbc.util.SchemaFields.idFieldsFor;

class SelectMaxIdQuery extends AbstractStorableSqlQuery {
    public SelectMaxIdQuery(Storable storable) {
        super(storable);
    }

    @Override
    protected String createParameterizedSql() {
        Optional<Schema.Field> sequenceField = getSequenceField(primaryKey);
        if (!sequenceField.isPresent()) {
            Set<Schema.Field> idFields = idFieldsFor(getStorable());
            if (idFields.isEmpty()) {
                throw new IllegalStateException("No simple primary key or field named 'id' extists for namespace " + getNamespace());
            }
            sequenceField = Optional.of(getOnlyElement(idFields));
        }
        String sql = String.format("SELECT MAX(%s) FROM %s", sequenceField.get().getName(), tableName);
        LOG.debug("Selecting max id for namespace {} with sql {}", getNamespace(), sql);
        return sql;
    }

    PreparedStatement prepareStatement(QueryExecutor queryExecutor) throws SQLException {
        return PreparedStatementBuilder.of(
                queryExecutor.getConnection(),
                new ExecutionConfig(1),
                new DefaultStorageDataTypeContext(),
                this).getPreparedStatement(this);
    }
}
