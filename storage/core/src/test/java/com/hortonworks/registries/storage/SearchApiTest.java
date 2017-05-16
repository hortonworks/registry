/*
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
package com.hortonworks.registries.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlSelectQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.postgresql.query.PostgresqlSelectQuery;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public class SearchApiTest {
    private static final Logger LOG = LoggerFactory.getLogger(SearchApiTest.class);
    private static SearchQuery complexQuery;
    private static SearchQuery simpleQuery;
    private static String complexQueryMySql;
    private static String simpleQueryMySql;
    private static String simpleQueryPostgreSql;
    private static String complexQueryPostgreSql;

    @BeforeClass
    public static void setup() {
        simpleQuery =
                SearchQuery
                        .searchFrom("foo")
                        .where(WhereClause.begin()
                                       .contains("name", "sato")
                                       .and()
                                       .gt("id", 0)
                                       .combine())
                        .orderBy(OrderBy.asc("name"));

        complexQuery = SearchQuery.searchFrom("store")
                .where(WhereClause.begin()
                               .enclose(WhereClause.begin()
                                                .eq("name", "foo")
                                                .and()
                                                .gt("amount", 500))
                               .or()
                               .enclose(WhereClause.begin()
                                                .contains("description", "bar")
                                                .and()
                                                .gt("count", 100))
                               .combine()

                ).orderBy(OrderBy.asc("name"));
        simpleQueryMySql = "SELECT * FROM foo WHERE  `name` LIKE '%sato%' AND `id` > ?  ORDER BY `name` ASC";
        complexQueryMySql = "SELECT * FROM store WHERE  `description` LIKE '%bar%' AND `count` > ?  ORDER BY `name` ASC ";

        simpleQueryPostgreSql = "SELECT * FROM foo WHERE  \"name\" LIKE '%sato%' AND \"id\" > ?  ORDER BY \"name\" ASC";
        complexQueryPostgreSql = "SELECT * FROM store WHERE  \"description\" LIKE '%bar%' AND \"count\" > ?  ORDER BY \"name\" ASC ";

    }

    @Test
    public void testGeneratedSqlForSearchApi() {
        List<Pair<SearchQuery, Pair<String, String>>> pairs = Lists.newArrayList(Pair.of(simpleQuery, Pair.of(simpleQueryMySql, simpleQueryPostgreSql)),
                                                                   Pair.of(complexQuery, Pair.of(complexQueryMySql, complexQueryPostgreSql)));
        final Schema schema = new DeviceInfo().getSchema();
        for (Pair<SearchQuery, Pair<String, String>> pair : pairs) {
            MySqlSelectQuery mySqlSelectQuery = new MySqlSelectQuery(pair.getLeft(), schema);
            String parametrizedSql = mySqlSelectQuery.getParametrizedSql();
            Assert.assertEquals(pair.getRight().getLeft().trim(), parametrizedSql.trim());

            PostgresqlSelectQuery postgresqlSelectQuery = new PostgresqlSelectQuery(pair.getLeft(),schema);
            String parametrizedPostgreSql = postgresqlSelectQuery.getParametrizedSql();
            Assert.assertEquals(pair.getRight().getRight().trim(), parametrizedPostgreSql.trim());
        }
    }

    @Test
    public void testSearchAPIJsons() throws Exception {
        LOG.info("simpleQuery = [{}]", simpleQuery);
        LOG.info("complexQuery = [{}]", complexQuery);

        SearchQuery[] queries = {simpleQuery, complexQuery};
        for (SearchQuery query : queries) {
            ObjectMapper objectMapper = new ObjectMapper();
            String queryAsJson = objectMapper.writeValueAsString(query);
            LOG.info("queryAsJson = [{}]", queryAsJson);

            SearchQuery returnedQuery = objectMapper.readValue(queryAsJson, SearchQuery.class);
            LOG.info("returnedQuery [{}] ", returnedQuery);

            Assert.assertEquals(query, returnedQuery);
        }
    }
}
