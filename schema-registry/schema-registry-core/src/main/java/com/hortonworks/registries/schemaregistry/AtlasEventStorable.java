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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.hortonworks.registries.storage.impl.jdbc.util.Util.numericToBoolean;

public class AtlasEventStorable extends AbstractStorable {

    public enum EventType {
        CREATE_META(1),
        UPDATE_META(2),
        CREATE_VERSION(3);

        final int numValue;

        EventType(int numValue) {
            this.numValue = numValue;
        }

        public int getNumValue() {
            return numValue;
        }

        @Nonnull
        public static EventType forNumValue(int value) {
            for (EventType et : EventType.values()) {
                if (et.getNumValue() == value) {
                    return et;
                }
            }
            throw new IllegalArgumentException("Unsupported numeric value for event type: " + value);
        }
    }

    public static final String NAME_SPACE = "atlas_events";

    public static final String ID = "id";
    public static final String USERNAME = "username";
    public static final String PROCESSED_ID = "processedId";
    public static final String TYPE = "type";
    public static final String PROCESSED = "processed";
    public static final String FAILED = "failed";
    public static final String TIMESTAMP = "timestamp";

    private Long id;
    private String username;
    private Long processedId;
    private Integer type;
    private boolean processed;
    private boolean failed;
    private Long timestamp;

    public AtlasEventStorable() { }

    public AtlasEventStorable(Long id) {
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

    public void setId(BigDecimal id) {
        setId(id.longValueExact());
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Long getProcessedId() {
        return processedId;
    }

    public void setProcessedId(Long processedId) {
        this.processedId = processedId;
    }

    public void setProcessedId(BigDecimal processedId) {
        setProcessedId(processedId.longValueExact());
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

    public void setProcessed(Boolean processed) {
        this.processed = processed;
    }

    public void setProcessed(BigDecimal processed) {
        setProcessed(numericToBoolean(processed));
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public void setType(BigDecimal type) {
        setType(type.intValueExact());
    }

    public void setType(@Nullable EventType et) {
        if (et == null) {
            this.type = null;
        } else {
            this.type = et.getNumValue();
        }
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

    public void setFailed(Boolean failed) {
        this.failed = failed;
    }

    public void setFailed(BigDecimal failed) {
        setFailed(numericToBoolean(failed));
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setTimestamp(BigDecimal timestamp) {
        setTimestamp(timestamp.longValueExact());
    }

    @Override
    public String toString() {
        return "AtlasEventStorable{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", processedId='" + processedId + '\'' +
                ", type=" + type +
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
        AtlasEventStorable that = (AtlasEventStorable) o;
        return processed == that.processed &&
                failed == that.failed &&
                Objects.equals(id, that.id) &&
                Objects.equals(username, that.username) &&
                Objects.equals(processedId, that.processedId) &&
                Objects.equals(type, that.type) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, username, processedId, type, processed, failed, timestamp);
    }
}
