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

package com.hortonworks.registries.storage.impl.jdbc.provider.oracle.statement;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.DefaultStorageDataTypeContext;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleDataTypeContext extends DefaultStorageDataTypeContext {

    private static final String EMPTY_STRING_PLACEHOLDER = "<EMPTY>";

    @Override
    public void setPreparedStatementParams(PreparedStatement preparedStatement,
                                           Schema.Type type, int index, Object val) throws SQLException {
        if (type == Schema.Type.STRING && StringUtils.isEmpty((String) val)) {
            preparedStatement.setString(index, EMPTY_STRING_PLACEHOLDER);
        } else {
            super.setPreparedStatementParams(preparedStatement, type, index, val);
        }
    }

    protected Object getJavaObject(Class columnJavaType, String columnLabel, ResultSet resultSet) throws SQLException {
        if (columnJavaType.equals(String.class)) {
            String stringValue = resultSet.getString(columnLabel);
            if (stringValue != null && stringValue.equals(EMPTY_STRING_PLACEHOLDER)) {
                return "";
            }
        }
        return super.getJavaObject(columnJavaType, columnLabel, resultSet);
    }
}
