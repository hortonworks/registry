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
package com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query;

import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.StorableKey;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OracleSelectQueryTest {

    private static final String NAME_SPACE = "topic";

    private final Map<Schema.Field, Object> fieldToObjectMap = ImmutableMap.of(new Schema.Field("foo", Schema.Type.LONG), 1);

    @Test
    public void testSelectForUpdate() {
        String parametrizedSql = new OracleSelectForUpdateQuery(storableKey()).getParametrizedSql();
        assertEquals("SELECT * FROM \"topic\" WHERE  \"foo\" = ? FOR UPDATE SKIP LOCKED", parametrizedSql);
    }

    @Test
    public void testSelectForShare() {
        String parametrizedSql = new OracleSelectForShareQuery(storableKey()).getParametrizedSql();
        assertEquals("SELECT * FROM \"topic\" WHERE  \"foo\" = ? FOR SHARE", parametrizedSql);
    }

    private StorableKey storableKey() {
        return new StorableKey(NAME_SPACE, new PrimaryKey(fieldToObjectMap));
    }
}