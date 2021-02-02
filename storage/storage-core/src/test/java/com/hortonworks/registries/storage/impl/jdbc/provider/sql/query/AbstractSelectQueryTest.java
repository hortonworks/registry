/*
 * Copyright 2016-2021 Cloudera, Inc.
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
 */
package com.hortonworks.registries.storage.impl.jdbc.provider.sql.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class AbstractSelectQueryTest {
    
    @Test
    public void buildSqlWithSearchQueryColumnOrderMatters() {
        //given
        WhereClause whereClause = WhereClause.begin().contains("description", "someDescription").or().contains("name", "someName").combine();
        SearchQuery searchQuery = SearchQuery.searchFrom("table");
        searchQuery.where(whereClause);
        Schema schema = new Schema();
        schema.setFields(Arrays.asList(Schema.Field.fromString("name='description', type=STRING"), 
                Schema.Field.fromString("name='name', type=STRING")));
        AbstractSelectQuery underTest = new SqlSelectQuery("table");
        List<Schema.Field> expected = Arrays.asList(Schema.Field.fromString("name='description', type=STRING"), 
                Schema.Field.fromString("name='name', type=STRING"));
        
        //when
        underTest.buildSqlWithSearchQuery(searchQuery, schema);
        
        //then
        Assertions.assertIterableEquals(expected, underTest.getColumns());
    }
}
