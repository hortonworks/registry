package com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.util.List;

public class OracleSelectForUpdateQuery extends OracleSelectQuery {
    public OracleSelectForUpdateQuery(String nameSpace) {
        super(nameSpace);
    }

    public OracleSelectForUpdateQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public OracleSelectForUpdateQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public OracleSelectForUpdateQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
    }

    public OracleSelectForUpdateQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery, schema);
    }

    @Override
    protected String getParameterizedSql() {
        String sql = super.getParameterizedSql() + " FOR UPDATE";
        LOG.debug(sql);
        return sql;
    }
}
