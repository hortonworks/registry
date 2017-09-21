/*
 * Copyright 2016 Hortonworks.
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

package com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.exception.OracleQueryException;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.AbstractSelectQuery;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OracleSelectQuery extends AbstractSelectQuery {

    public OracleSelectQuery(String nameSpace) {
        super(nameSpace);
    }

    public OracleSelectQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public OracleSelectQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public OracleSelectQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
    }

    public OracleSelectQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery, schema);
    }

    @Override
    protected String fieldEncloser() {
        return "\"";
    }

    @Override
    protected String tableNameEncloser() {
        return "\"";
    }

    @Override
    protected void initParameterizedSql() {
        sql = "SELECT * FROM " + tableNameEncloser() + tableName + tableNameEncloser();
        if (columns != null) {
            List<String> whereClauseColumns = new LinkedList<>();
            for (Map.Entry<Schema.Field, Object> columnKeyValue : primaryKey.getFieldsToVal().entrySet()) {
                if (columnKeyValue.getKey().getType() == Schema.Type.STRING) {
                    String stringValue = (String) columnKeyValue.getValue();
                    if ((stringValue).length() > 4000) {
                        throw new OracleQueryException(String.format("Column \"%s\" of the table \"%s\" is compared against a value \"%s\", " +
                                        "which is greater than 4k characters",
                                columnKeyValue.getKey().getName(), tableName, stringValue));
                    } else
                        whereClauseColumns.add(String.format(" to_char(\"%s\") = ?", columnKeyValue.getKey().getName()));
                } else {
                    whereClauseColumns.add(String.format(" \"%s\" = ?", columnKeyValue.getKey().getName()));
                }
            }
            sql += " WHERE " + join(whereClauseColumns, " AND ");
        }
        log.debug(sql);
    }

    protected void addOrderByFieldsToParameterizedSql() {
        if (orderByFields != null && !orderByFields.isEmpty()) {
            sql += join(orderByFields.stream()
                    .map(x -> " ORDER BY " + fieldEncloser() + x.getFieldName() + fieldEncloser() + " " + (x.isDescending() ? "DESC" : "ASC"))
                    .collect(Collectors.toList()), ",");
        }

        log.debug("SQL after adding order by clause: [{}]", sql);
    }
}
