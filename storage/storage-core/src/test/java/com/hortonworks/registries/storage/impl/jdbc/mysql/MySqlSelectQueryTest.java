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
 package com.hortonworks.registries.storage.impl.jdbc.mysql;

 import com.google.common.collect.ImmutableMap;
 import com.hortonworks.registries.common.Schema;
 import com.hortonworks.registries.storage.OrderByField;
 import com.hortonworks.registries.storage.PrimaryKey;
 import com.hortonworks.registries.storage.StorableKey;
 import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlSelectForShareQuery;
 import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlSelectForUpdateQuery;
 import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlSelectQuery;
 import com.hortonworks.registries.storage.search.OrderBy;
 import com.hortonworks.registries.storage.search.SearchQuery;
 import com.hortonworks.registries.storage.search.WhereClause;
 import org.junit.jupiter.api.Assertions;
 import org.junit.jupiter.api.DisplayName;
 import org.junit.jupiter.api.Nested;
 import org.junit.jupiter.api.Test;

 import java.util.Arrays;
 import java.util.List;
 import java.util.Map;

 import static org.junit.jupiter.api.Assertions.assertEquals;

 public class MySqlSelectQueryTest {
     private static final String NAME_SPACE = "topic";

     private final Map<Schema.Field, Object> fieldToObjectMap = ImmutableMap.of(new Schema.Field("foo", Schema.Type.LONG), 1);

     @Test
     public void testSelectQuery() {
         MySqlSelectQuery mySqlSelectQuery = new MySqlSelectQuery(NAME_SPACE);
         String parametrizedSql = mySqlSelectQuery.getParametrizedSql();

         Assertions.assertEquals("SELECT * FROM topic", parametrizedSql);

         mySqlSelectQuery = new MySqlSelectQuery(storableKey());
         parametrizedSql = mySqlSelectQuery.getParametrizedSql();

         Assertions.assertEquals("SELECT * FROM topic WHERE `foo` = ?", parametrizedSql);
     }

     @Test
     public void testSelectQueryWithOrderBy() {
         List<OrderByField> orderByFields = Arrays.asList(OrderByField.of("foo", true),
                 OrderByField.of("bar"));
         MySqlSelectQuery mySqlSelectQuery = new MySqlSelectQuery("topic", orderByFields);
         String parametrizedSql = mySqlSelectQuery.getParametrizedSql();

         Assertions.assertEquals("SELECT * FROM topic ORDER BY `foo` DESC, ORDER BY `bar` ASC", parametrizedSql);

         mySqlSelectQuery = new MySqlSelectQuery(storableKey(), orderByFields);
         parametrizedSql = mySqlSelectQuery.getParametrizedSql();

         Assertions.assertEquals("SELECT * FROM topic WHERE `foo` = ? ORDER BY `foo` DESC, ORDER BY `bar` ASC", parametrizedSql);
     }

     @Test
     public void testSelectForUpdate() {
         String parametrizedSql = new MySqlSelectForUpdateQuery(storableKey()).getParametrizedSql();
         assertEquals("SELECT * FROM topic WHERE `foo` = ? FOR UPDATE", parametrizedSql);
     }

     @Test
     public void testSelectForShare() {
         String parametrizedSql = new MySqlSelectForShareQuery(storableKey()).getParametrizedSql();
         assertEquals("SELECT * FROM topic WHERE `foo` = ? FOR SHARE", parametrizedSql);
     }

     @Nested
     @DisplayName("search query")
     class SearchQueryTest {

         private final Schema schema = Schema.of(Schema.Field.of("name", Schema.Type.STRING), Schema.Field.of("amount", Schema.Type.LONG));

         @Test
         void testSearchQuery() {
             SearchQuery searchQuery = SearchQuery.searchFrom("store")
                     .where(WhereClause.begin()
                             .contains("name", "foo")
                             .and()
                             .gt("amount", 500)
                             .combine()
                     ).orderBy(OrderBy.asc("name"), OrderBy.desc("amount"));

             String generatedSql = new MySqlSelectQuery(searchQuery, schema).getParametrizedSql();
             String expectedSql = "SELECT * FROM `store` WHERE `name` LIKE ? AND `amount` > ?  ORDER BY `name` ASC , `amount` DESC ";
             assertEquals(expectedSql, generatedSql);
         }

         @Test
         void testForUpdateSearchQuery() {
             SearchQuery searchQuery = SearchQuery.searchFrom("store")
                     .where(WhereClause.begin()
                             .contains("name", "foo")
                             .and()
                             .gt("amount", 500)
                             .combine()
                     ).orderBy(OrderBy.asc("name"), OrderBy.desc("amount"))
                     .forUpdate();

             String generatedSql = new MySqlSelectQuery(searchQuery, schema).getParametrizedSql();
             String expectedSql = "SELECT * FROM `store` WHERE `name` LIKE ? AND `amount` > ?  ORDER BY `name` ASC , `amount` DESC FOR UPDATE";
             assertEquals(expectedSql, generatedSql);
         }
     }

     private StorableKey storableKey() {
         return new StorableKey(NAME_SPACE, new PrimaryKey(fieldToObjectMap));
     }
 }
