 /*
 * Copyright 2017-2021 Cloudera, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.storage.impl.jdbc.postgres;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.impl.jdbc.provider.postgresql.query.PostgresqlSelectQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PostgresSelectQueryTest {
    private static final String NAME_SPACE = "topic";

    @Test
    public void testSelectQuery() throws Exception {
        PostgresqlSelectQuery postgresqlSelectQuery = new PostgresqlSelectQuery(NAME_SPACE);
        String parametrizedSql = postgresqlSelectQuery.getParametrizedSql();

        Assertions.assertEquals("SELECT * FROM \"topic\"", parametrizedSql);

        Map<Schema.Field, Object> fieldToObjectMap = new HashMap<>();
        fieldToObjectMap.put(new Schema.Field("foo", Schema.Type.LONG), 1);

        postgresqlSelectQuery = new PostgresqlSelectQuery(new StorableKey(NAME_SPACE, new PrimaryKey(fieldToObjectMap)));
        parametrizedSql = postgresqlSelectQuery.getParametrizedSql();

        Assertions.assertEquals("SELECT * FROM \"topic\" WHERE \"foo\" = ?", parametrizedSql);
    }

    @Test
    public void testSelectQueryWithOrderBy() throws Exception {
        List<OrderByField> orderByFields = Arrays.asList(OrderByField.of("foo", true),
                                                         OrderByField.of("bar"));
        PostgresqlSelectQuery postgresqlSelectQuery = new PostgresqlSelectQuery("topic", orderByFields);
        String parametrizedSql = postgresqlSelectQuery.getParametrizedSql();

        Assertions.assertEquals("SELECT * FROM \"topic\" ORDER BY \"foo\" DESC, ORDER BY \"bar\" ASC", parametrizedSql);

        Map<Schema.Field, Object> fieldToObjectMap = new HashMap<>();
        fieldToObjectMap.put(new Schema.Field("foo", Schema.Type.LONG), 1);

        postgresqlSelectQuery = new PostgresqlSelectQuery(new StorableKey(NAME_SPACE, new PrimaryKey(fieldToObjectMap)), orderByFields);
        parametrizedSql = postgresqlSelectQuery.getParametrizedSql();

        Assertions.assertEquals("SELECT * FROM \"topic\" WHERE \"foo\" = ? ORDER BY \"foo\" DESC, ORDER BY \"bar\" ASC", parametrizedSql);

    }
}
