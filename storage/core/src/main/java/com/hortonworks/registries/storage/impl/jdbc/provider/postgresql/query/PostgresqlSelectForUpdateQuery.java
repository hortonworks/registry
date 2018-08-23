package com.hortonworks.registries.storage.impl.jdbc.provider.postgresql.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.util.List;

public class PostgresqlSelectForUpdateQuery extends PostgresqlSelectQuery {
    public PostgresqlSelectForUpdateQuery(String nameSpace) {
        super(nameSpace);
    }

    public PostgresqlSelectForUpdateQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public PostgresqlSelectForUpdateQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public PostgresqlSelectForUpdateQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
    }

    public PostgresqlSelectForUpdateQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery, schema);
    }

    @Override
    protected String getParameterizedSql() {
        String sql = super.getParameterizedSql() + " FOR UPDATE";
        LOG.debug(sql);
        return sql;
    }
}
