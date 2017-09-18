/**
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.storage.impl.jdbc.provider.phoenix.query;

import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.AbstractSelectQuery;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class PhoenixSelectQuery extends AbstractSelectQuery {

    public PhoenixSelectQuery(String nameSpace) {
        super(nameSpace);
    }

    public PhoenixSelectQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public PhoenixSelectQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public PhoenixSelectQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
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
    protected void addOrderByFieldsToParameterizedSql() {
        if (orderByFields != null && !orderByFields.isEmpty()) {
            sql += join(orderByFields.stream()
                                .map(x -> " ORDER BY \"" + x.getFieldName() + "\" " + (x.isDescending() ? "DESC" : "ASC"))
                                .collect(Collectors.toList()), ",");
        }
        log.debug("SQL after adding order by clause: [{}]", sql);
    }

    @Override
    protected void initParameterizedSql() {
        sql = "SELECT * FROM " + tableName;
        //where clause is defined by columns specified in the PrimaryKey
        if (columns != null) {
            sql += " WHERE " + join(getColumnNames(columns, "\"%s\" = ?"), " AND ");
        }
        log.debug(sql);
    }
}
