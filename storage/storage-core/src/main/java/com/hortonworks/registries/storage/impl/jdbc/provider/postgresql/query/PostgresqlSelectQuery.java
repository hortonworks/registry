/*
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
 */
package com.hortonworks.registries.storage.impl.jdbc.provider.postgresql.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.AbstractSelectQuery;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class PostgresqlSelectQuery extends AbstractSelectQuery {

    public PostgresqlSelectQuery(String nameSpace) {
        super(nameSpace);
    }

    public PostgresqlSelectQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public PostgresqlSelectQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public PostgresqlSelectQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
    }

    public PostgresqlSelectQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery, schema);
    }

    @Override
    protected String fieldEncloser() {
        return "\"";
    }

    @Override
    public String orderBySql() {
        String sql = "";
        if (orderByFields != null && !orderByFields.isEmpty()) {
            sql += join(orderByFields.stream()
                                .map(x -> " ORDER BY \"" + x.getFieldName() + "\" " + (x.isDescending() ? "DESC" : "ASC"))
                                .collect(Collectors.toList()), ",");
        }
        return sql;
    }

    @Override
    protected String getParameterizedSql() {
        String sql = "SELECT * FROM \"" + tableName + "\"";
        if (columns != null) {
            sql += " WHERE " + join(getColumnNames(columns, "\"%s\" = ?"), " AND ");
        }
        LOG.debug(sql);
        return sql;
    }

}
