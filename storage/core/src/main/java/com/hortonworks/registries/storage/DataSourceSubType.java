/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a subtype of DataSource like 'Device'.
 * The dataSourceId is the foreign key reference to the
 * parent entity DataSource.
 */
public abstract class DataSourceSubType extends AbstractStorable {
    public static final String DATA_SOURCE_ID = "dataSourceId";

    /**
     * Primary key that is also a foreign key to referencing to the parent table 'dataSources'.
     */
    protected Long dataSourceId;

    /**
     * The primary key of the device is the datasource id itself which is also a foreign key
     * reference to the parent 'DataSource'.
     */
    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> fieldToObjectMap = new HashMap<>();
        fieldToObjectMap.put(new Schema.Field(DATA_SOURCE_ID, Schema.Type.LONG), dataSourceId);
        return new PrimaryKey(fieldToObjectMap);
    }

    /**
     * Get the id of its parent data source.
     */
    @JsonIgnore
    public Long getDataSourceId() {
        return dataSourceId;
    }

    /**
     * Set the id of its parent data source.
     */
    public void setDataSourceId(Long dataSourceId) {
        this.dataSourceId = dataSourceId;
    }
}
