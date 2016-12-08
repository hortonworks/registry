package com.hortonworks.registries.storage.impl.jdbc.provider.phoenix.query;

import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.AbstractStorableKeyQuery;

/**
 *
 */
public class PhoenixDeleteQuery extends AbstractStorableKeyQuery {

    public PhoenixDeleteQuery(String nameSpace) {
        super(nameSpace);
    }

    public PhoenixDeleteQuery(StorableKey storableKey) {
        super(storableKey);
    }

    @Override
    protected void setParameterizedSql() {
        sql = "DELETE FROM  " + tableName + " WHERE " + join(getColumnNames(columns, "\"%s\" = ?"), " AND ");
        log.debug(sql);
    }
}
