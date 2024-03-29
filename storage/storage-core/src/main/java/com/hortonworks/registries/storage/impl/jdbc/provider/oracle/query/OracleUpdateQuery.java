/**
 * Copyright 2017-2019 Cloudera, Inc.
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

package com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.exception.OracleQueryException;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.AbstractStorableUpdateQuery;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OracleUpdateQuery extends AbstractStorableUpdateQuery {

    private Map<Schema.Field, Object> whereClauseColumnToValueMap = new HashMap<>();

    public OracleUpdateQuery(Storable storable) {
        super(storable);
        whereClauseColumnToValueMap = createWhereClauseColumnToValueMap();
    }

    @Override
    protected String createParameterizedSql() {

        List<String> whereClauseColumns = new LinkedList<>();
        for (Map.Entry<Schema.Field, Object> columnKeyValue : whereClauseColumnToValueMap.entrySet()) {
            if (columnKeyValue.getKey().getType() == Schema.Type.STRING) {
                String stringValue = (String) columnKeyValue.getValue();
                if ((stringValue).length() > 4000) {
                    throw new OracleQueryException(String.format("Column \"%s\" of the table \"%s\" is compared against a value \"%s\", " +
                                    "which is greater than 4k characters",
                            columnKeyValue.getKey().getName(), tableName, stringValue));
                } else {
                    whereClauseColumns.add(String.format(" to_char(\"%s\") = ?", columnKeyValue.getKey().getName()));
                }
            } else {
                whereClauseColumns.add(String.format(" \"%s\" = ?", columnKeyValue.getKey().getName()));
            }
        }

        String sql = "UPDATE \"" + tableName + "\" SET "
                + join(getColumnNames(columns, "\"%s\" = ?"), ", ")
                + " WHERE " + join(whereClauseColumns, " AND ");
        LOG.debug("Sql '{}'", sql);
        return sql;
    }

    private Map<Schema.Field, Object> createWhereClauseColumnToValueMap() {
        Map<Schema.Field, Object> bindingMap = new HashMap<>();
        for (Pair<Schema.Field, Object> fieldObjectPair: getBindings()) {
            bindingMap.put(fieldObjectPair.getKey(), fieldObjectPair.getValue());
        }
        return whereFields.stream().collect(Collectors.toMap(f -> f, f -> bindingMap.get(f)));
    }
}
