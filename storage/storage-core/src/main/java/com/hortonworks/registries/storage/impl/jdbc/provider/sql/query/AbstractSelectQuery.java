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
package com.hortonworks.registries.storage.impl.jdbc.provider.sql.query;

import com.google.common.collect.Lists;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.Predicate;
import com.hortonworks.registries.storage.search.PredicateCombinerPair;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import com.hortonworks.registries.storage.search.WhereClauseCombiner;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class AbstractSelectQuery extends AbstractStorableKeyQuery {

    protected List<OrderByField> orderByFields;

    protected SearchQuery searchQuery;
    protected Schema schema;
    protected boolean lockRows = false;

    public AbstractSelectQuery(String nameSpace) {
        this(nameSpace, null);
    }

    public AbstractSelectQuery(StorableKey storableKey) {
        this(storableKey, null);
    }

    public AbstractSelectQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace);
        this.orderByFields = orderByFields;
    }

    public AbstractSelectQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey);
        this.orderByFields = orderByFields;
    }

    public AbstractSelectQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery.getNameSpace());
        this.searchQuery = searchQuery;
        this.schema = schema;
        this.lockRows = searchQuery.isForUpdate();
    }

    protected abstract String getParameterizedSql();

    protected abstract String orderBySql();

    protected String lockingBehaviorClause() {
        return "FOR UPDATE SKIP LOCKED";
    }

    public boolean isLockRows() {
        return lockRows;
    }

    public void setLockRows(boolean lockRows) {
        this.lockRows = lockRows;
    }

    @Override
    protected final String createParameterizedSql() {
        if (searchQuery != null) {
            return appendLockingClause(buildSqlWithSearchQuery(searchQuery, schema));
        } else {
            String sql = getParameterizedSql();
            String orderBy = orderBySql();
            if (!StringUtils.isEmpty(orderBy)) {
                sql += orderBy;
            }

            return appendLockingClause(sql);
        }
    }

    private String appendLockingClause(String sql) {
        if (lockRows) {
            return sql.trim() + " " + lockingBehaviorClause();
        } else {
            return sql;
        }
    }

    protected String buildSqlWithSearchQuery(SearchQuery searchQuery, Schema schema) {
        String sql = "SELECT * FROM " + fieldEncloser() + tableName + fieldEncloser();

        WhereClause whereClause = searchQuery.getWhereClause();
        Map<Schema.Field, Object> fieldsToValues = new LinkedHashMap<Schema.Field, Object>() { };
        if (whereClause != null) {
            sql += " WHERE";
            StringBuilder clauseString = new StringBuilder();
            for (PredicateCombinerPair predicateCombinerPair : whereClause.getPredicateCombinerPairs()) {
                WhereClauseCombiner.Operation combinerOperation = predicateCombinerPair.getCombinerOperation();

                Predicate predicate = predicateCombinerPair.getPredicate();
                clauseString.append(generateClauseString(predicate, fieldsToValues, schema));
                if (combinerOperation != null) {
                    String opStr;
                    switch (combinerOperation) {
                        case ENCL_START:
                            opStr = " ( ";
                            break;
                        case ENCL_FINISH:
                            opStr = " ) ";
                            break;
                        default:
                            opStr = " " + combinerOperation.toString();
                            break;
                    }
                    clauseString.append(opStr);
                }
            }
            sql += clauseString;
        }

        List<OrderBy> orderByFields = searchQuery.getOrderByFields();
        if (orderByFields != null && !orderByFields.isEmpty()) {
            sql += " ORDER BY "
                    + join(orderByFields
                                   .stream()
                                   .map(x -> fieldEncloser() + x.getFieldName() + fieldEncloser() + (x.isAsc() ? " ASC " : " DESC "))
                                   .collect(Collectors.toList()), ", "
            );
        }

        primaryKey = new PrimaryKey(fieldsToValues);
        columns = Lists.newArrayList(fieldsToValues.keySet());

        return sql;
    }

    protected abstract String fieldEncloser();

    private String generateClauseString(Predicate predicate, Map<Schema.Field, Object> fieldsToValues, Schema schema) {
        if (predicate == null) {
            return "";
        }

        String result;
        Predicate.Operation operation = predicate.getOperation();
        String fq = fieldEncloser();

        Object predicateValue = predicate.getValue();
        switch (operation) {
            case CONTAINS:
                result = " " + fq + predicate.getField() + fq + " LIKE ?";
                predicateValue = "%" + predicateValue + "%";
                break;
            case EQ:
                result = " " + fq + predicate.getField() + fq + " = ? ";
                break;
            case GT:
                result = " " + fq + predicate.getField() + fq + " > ? ";
                break;
            case GTE:
                result = " " + fq + predicate.getField() + fq + " >= ? ";
                break;
            case LT:
                result = " " + fq + predicate.getField() + fq + " < ? ";
                break;
            case LTE:
                result = " " + fq + predicate.getField() + fq + " <= ? ";
                break;
            default:
                throw new IllegalArgumentException("Given operation " + operation + " is not supported!");
        }

        Schema.Field field = schema.getField(predicate.getField());
        fieldsToValues.put(field, predicateValue);

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        AbstractSelectQuery that = (AbstractSelectQuery) o;

        return orderByFields != null ? orderByFields.equals(that.orderByFields) : that.orderByFields == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (orderByFields != null ? orderByFields.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AbstractSelectQuery{" +
                "orderByFields=" + orderByFields +
                '}' + super.toString();
    }
}
