package com.hortonworks.registries.storage.impl.jdbc.provider.sql.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

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
        assertThat(underTest.getColumns(), is(expected));
    }
}
