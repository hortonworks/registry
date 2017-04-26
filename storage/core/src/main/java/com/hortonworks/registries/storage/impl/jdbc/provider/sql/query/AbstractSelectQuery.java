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

import com.hortonworks.registries.storage.StorableKey;

import java.util.List;

/**
 *
 */
public abstract class AbstractSelectQuery extends AbstractStorableKeyQuery {

    protected final List<OrderByField> orderByFields;

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

    protected abstract void addOrderByFieldsToParameterizedSql();

}
