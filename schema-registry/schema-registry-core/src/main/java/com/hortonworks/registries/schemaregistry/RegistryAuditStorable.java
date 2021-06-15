/**
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
 **/
package com.hortonworks.registries.schemaregistry;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RegistryAuditStorable extends AbstractStorable {

    public static final String NAME_SPACE = "registry_audit";

    public static final String ID = "id";
    public static final String USERNAME = "username";
    public static final String PROCESSED_DATA = "processed_data";
    public static final String PROCESSED = "processed";
    public static final String FAILED = "failed";
    public static final String TIMESTAMP = "timestamp";

    private Long id;
    private String username;
    private String processedData;
    private boolean processed;
    private boolean failed;
    private Long timestamp;

    public RegistryAuditStorable() { }

    public RegistryAuditStorable(Long id) {
        this.id = id;
    }

    @Override
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(new Schema.Field(SchemaFieldInfo.ID, Schema.Type.LONG), id);
        return new PrimaryKey(values);
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getProcessedData() {
        return processedData;
    }

    public void setProcessedData(String processedData) {
        this.processedData = processedData;
    }

    public boolean isProcessed() {
        return processed;
    }

    public boolean getProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public boolean isFailed() {
        return failed;
    }

    public boolean getFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "RegistryAuditStorable{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", processedData='" + processedData + '\'' +
                ", processed=" + processed +
                ", failed=" + failed +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RegistryAuditStorable that = (RegistryAuditStorable) o;
        return processed == that.processed &&
                failed == that.failed &&
                Objects.equals(id, that.id) &&
                Objects.equals(username, that.username) &&
                Objects.equals(processedData, that.processedData) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, username, processedData, processed, failed, timestamp);
    }
}
