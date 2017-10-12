/**
 * Copyright 2017 Hortonworks.
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

import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.statement.OracleDataTypeContext;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.AbstractSqlQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleSequenceIdQuery {
    private static final Logger log = LoggerFactory.getLogger(OracleSequenceIdQuery.class);
    private static final String nextValueFunction = "nextval";
    private static final String SEQUENCE_SUFFIX = "__sequence";
    private String namespace;
    private ConnectionBuilder connectionBuilder;
    private int queryTimeoutSecs;
    private final OracleDataTypeContext oracleDataTypeContext;

    public OracleSequenceIdQuery(String namespace, ConnectionBuilder connectionBuilder, int queryTimeoutSecs, OracleDataTypeContext oracleDataTypeContext) {
        this.namespace = namespace;
        this.connectionBuilder = connectionBuilder;
        this.queryTimeoutSecs = queryTimeoutSecs;
        this.oracleDataTypeContext = oracleDataTypeContext;
    }

    public Long getNextID() {

        OracleSqlQuery nextValueQuery = new OracleSqlQuery(String.format("SELECT %s%s.%s from DUAL", namespace, SEQUENCE_SUFFIX, nextValueFunction));
        Long nextId = 0l;

        try (Connection connection = connectionBuilder.getConnection()) {
            ResultSet selectResultSet = PreparedStatementBuilder.of(connection, new ExecutionConfig(queryTimeoutSecs), oracleDataTypeContext, nextValueQuery).getPreparedStatement(nextValueQuery).executeQuery();
            if (selectResultSet.next()) {
                nextId = selectResultSet.getLong(nextValueFunction);
            } else {
                throw new RuntimeException("No sequence-id created for the current sequence of [" + namespace + "]");
            }
            log.debug("Generated sequence id [{}] for [{}]", nextId, namespace);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        return nextId;
    }

    static class OracleSqlQuery extends AbstractSqlQuery {

        public OracleSqlQuery(String sql) {
            this.sql = sql;
        }

        @Override
        protected void initParameterizedSql() {
        }
    }
}
