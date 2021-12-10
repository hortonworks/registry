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
package com.hortonworks.registries.storage.impl.jdbc.provider.sql.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;

import java.util.Collections;

import static java.util.Collections.emptyMap;

public abstract class AbstractAggregateSqlQuery extends AbstractSelectQuery {

    private final boolean quoteTableName;
    private final String aggregationFunction;

    public AbstractAggregateSqlQuery(String namespace, Schema.Field field, boolean quoteTableName, String aggregationFunction) {
        super(namespace);
        this.columns = Collections.singletonList(field);
        this.quoteTableName = quoteTableName;
        this.aggregationFunction = aggregationFunction;
    }

    public Schema.Field getField() {
        return columns.iterator().next();
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return new PrimaryKey(emptyMap());
    }

    @Override
    protected String getParameterizedSql() {
        return String.format("SELECT %s(%s%s%s) FROM %s", aggregationFunction, fieldEncloser(), getField().getName(), fieldEncloser(), sqlTableName());
    }

    @Override
    protected String orderBySql() {
        return null;
    }

    private String sqlTableName() {
        if (quoteTableName) {
            return fieldEncloser() + tableName + fieldEncloser();
        } else {
            return tableName;
        }
    }
}
