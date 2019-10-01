/*
 * Copyright 2018-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaRegistryVersion {

    public static final SchemaRegistryVersion UNKNOWN = new SchemaRegistryVersion("unknown", "unknown", null);

    @JsonProperty
    private String version;

    @JsonProperty
    private String revision;

    @JsonProperty
    private Long timestamp;

    private SchemaRegistryVersion() {
    }

    public SchemaRegistryVersion(String version, String revision, Long timestamp) {
        this.version = version;
        this.revision = revision;
        this.timestamp = timestamp;
    }

    public String version() {
        return version;
    }

    public String revision() {
        return revision;
    }

    public Long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaRegistryVersion that = (SchemaRegistryVersion) o;
        return Objects.equals(version, that.version) &&
               Objects.equals(revision, that.revision) &&
               Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, revision, timestamp);
    }

    @Override
    public String toString() {
        return "SchemaRegistryVersion{" +
               "version='" + version + '\'' +
               ", revision='" + revision + '\'' +
               ", timestamp=" + timestamp +
               '}';
    }
}
