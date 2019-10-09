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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * This class encapsulates Schema Registry service information like version. Any other information can be added later.
 */
public final class SchemaRegistryServiceInfo {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryServiceInfo.class);

    private static final SchemaRegistryServiceInfo INSTANCE = new SchemaRegistryServiceInfo(fetchRegistryVersion());

    private SchemaRegistryVersion version;

    private static SchemaRegistryVersion fetchRegistryVersion() {
        try(InputStream inputStream = SchemaRegistryServiceInfo.class.getResourceAsStream("/registry/VERSION")) {
            Properties props = new Properties();
            props.load(inputStream);
            String version = props.getProperty("version", "unknown");
            String revision = props.getProperty("revision", "unknown");
            String timestampProp = props.getProperty("timestamp");
            Long timestamp = timestampProp != null ? Long.parseLong(timestampProp) : null;

            LOG.info("Schema Registry version [{}], revision [{}], timestamp [{}] ", version, revision,
                     timestamp != null ? new Date(timestamp) : null);

            return new SchemaRegistryVersion(version, revision, timestamp);
        } catch (Exception e) {
            LOG.warn("Error occurred while reading version file", e);
        }

        return SchemaRegistryVersion.UNKNOWN;
    }

    private SchemaRegistryServiceInfo(SchemaRegistryVersion version) {
        this.version = version;
    }

    public SchemaRegistryVersion version() {
        return version;
    }

    public static SchemaRegistryServiceInfo get() {
        return INSTANCE;
    }
}
