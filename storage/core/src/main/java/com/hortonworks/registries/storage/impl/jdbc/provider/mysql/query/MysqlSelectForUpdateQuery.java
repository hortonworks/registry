package com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.util.List;

public class MysqlSelectForUpdateQuery extends MySqlSelectQuery {

    public MysqlSelectForUpdateQuery(String nameSpace) {
        super(nameSpace);
    }

    public MysqlSelectForUpdateQuery(StorableKey storableKey) {
        super(storableKey);
    }

    public MysqlSelectForUpdateQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace, orderByFields);
    }

    public MysqlSelectForUpdateQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey, orderByFields);
    }

    public MysqlSelectForUpdateQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery, schema);
    }

    @Override
    protected String getParameterizedSql() {
        String sql = super.getParameterizedSql() + " FOR UPDATE";
        LOG.info(sql);
        return sql;
    }
}
