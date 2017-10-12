/**
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
 **/
package com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query;

import com.hortonworks.registries.storage.exception.NonIncrementalColumnException;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.MetadataHelper;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.DefaultStorageDataTypeContext;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.StorageDataTypeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class MySqlQueryUtils {
    private static final Logger log = LoggerFactory.getLogger(MySqlQueryUtils.class);

    private static final StorageDataTypeContext STORAGE_DATA_TYPE_CONTEXT = new DefaultStorageDataTypeContext();

    private MySqlQueryUtils() {
    }

    public static long nextIdMySql(Connection connection, String namespace, int queryTimeoutSecs) throws SQLException {
        if (!MetadataHelper.isAutoIncrement(connection, namespace, queryTimeoutSecs, STORAGE_DATA_TYPE_CONTEXT)) {
            throw new NonIncrementalColumnException();
        }

        final ResultSet resultSet = getResultSet(connection, queryTimeoutSecs, buildNextIdMySql(connection, namespace));
        resultSet.next();
        final long nextId = resultSet.getLong("AUTO_INCREMENT");
        log.debug("Next id for auto increment table [{}] = {}", namespace, nextId);
        return nextId;
    }

    private static ResultSet getResultSet(Connection connection, int queryTimeoutSecs, String sql) throws SQLException {
        final MySqlQuery sqlBuilder = new MySqlQuery(sql);
        return PreparedStatementBuilder.of(connection, new ExecutionConfig(queryTimeoutSecs), STORAGE_DATA_TYPE_CONTEXT,
                sqlBuilder).getPreparedStatement(sqlBuilder).executeQuery();
    }

    private static String buildNextIdMySql(Connection connection, String namespace) throws SQLException {
        final String database  = connection.getCatalog();
        final String sql = "SELECT AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"
                + namespace + "' AND TABLE_SCHEMA = '" + database + "'";
        log.debug("nextIdMySql() SQL query: {}", sql);
        return sql;
    }

    public static long nextIdH2(Connection connection, String namespace, int queryTimeoutSecs) throws SQLException {
        if (!MetadataHelper.isAutoIncrement(connection, namespace, queryTimeoutSecs, STORAGE_DATA_TYPE_CONTEXT)) {
            throw new NonIncrementalColumnException();
        }

        final ResultSet resultSet = getResultSet(connection, queryTimeoutSecs,
                buildNextIdH2(getH2SequenceName(connection, namespace, queryTimeoutSecs)));

        resultSet.next();
        final long nextId = resultSet.getLong("CURRENT_VALUE");
        log.debug("Next id for auto increment table [{}] = {}", namespace, nextId);
        return nextId;
    }

    private static String buildNextIdH2(String sequenceName) throws SQLException {
        final String sql = "SELECT CURRENT_VALUE FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_NAME = '" + sequenceName + "'";
        log.debug("nextIdH2() SQL query: {}", sql);
        return sql;
    }

    private static String getH2SequenceName(Connection connection, String namespace, int queryTimeoutSecs) throws SQLException {
        final ResultSet resultSet = getResultSet(connection, queryTimeoutSecs, getH2InfoSchemaSql(namespace));

        resultSet.next();
        String sql = resultSet.getString("SQL");

        Pattern p = Pattern.compile("(?i)(SYSTEM_SEQUENCE.*?)([)])");
        Matcher m = p.matcher(sql);
        String seqName = null;
        if (m.find()) {
            seqName = m.group(1);
            log.debug("SQL: {} => \n\t", sql, seqName);
        }

        if (seqName == null) {
            throw new RuntimeException("No sequence name found for namespace [" + namespace + "]");
        }
        return seqName;
    }

    private static String getH2InfoSchemaSql(String namespace) {
        return "SELECT `SQL` FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + namespace + "'";
    }
}
