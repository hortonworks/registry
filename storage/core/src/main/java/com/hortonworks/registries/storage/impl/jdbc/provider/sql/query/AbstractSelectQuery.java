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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class AbstractSelectQuery extends AbstractStorableKeyQuery {

    protected List<OrderByField> orderByFields;

    public AbstractSelectQuery(String nameSpace) {
        this(nameSpace, null);
    }

    public AbstractSelectQuery(StorableKey storableKey) {
        this(storableKey, null);
    }

    public AbstractSelectQuery(String nameSpace, List<OrderByField> orderByFields) {
        super(nameSpace);
        this.orderByFields = orderByFields;
        addOrderByFieldsToParameterizedSql();
    }

    public AbstractSelectQuery(StorableKey storableKey, List<OrderByField> orderByFields) {
        super(storableKey);
        this.orderByFields = orderByFields;
        addOrderByFieldsToParameterizedSql();
    }

    public AbstractSelectQuery(SearchQuery searchQuery, Schema schema) {
        super(searchQuery.getNameSpace());
        buildSqlWithSearchQuery(searchQuery, schema);
    }

    protected void buildSqlWithSearchQuery(SearchQuery searchQuery, Schema schema) {
        sql = "SELECT * FROM " + tableNameEncloser() + tableName + tableNameEncloser() ;

        WhereClause whereClause = searchQuery.getWhereClause();
        Map<Schema.Field, Object> fieldsToValues = new HashMap<>();
        if (whereClause != null) {
            sql += " WHERE ";
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
                            opStr = combinerOperation.toString();
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
    }

    protected abstract String fieldEncloser();

    protected abstract String tableNameEncloser();

    private String generateClauseString(Predicate predicate, Map<Schema.Field, Object> fieldsToValues, Schema schema) {
        if(predicate == null) {
            return "";
        }

        String result;
        Predicate.Operation operation = predicate.getOperation();
        String fq = fieldEncloser();
        boolean addToFieldValues = true;
        switch (operation) {
            case CONTAINS:
                result = " " + fq + predicate.getField() + fq + " LIKE '%" + predicate.getValue() + "%' ";
                addToFieldValues = false;
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

        if (addToFieldValues) {
            Schema.Field field = schema.getField(predicate.getField());
            fieldsToValues.put(field, predicate.getValue());
        }

        return result;
    }

    protected abstract void addOrderByFieldsToParameterizedSql();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

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
