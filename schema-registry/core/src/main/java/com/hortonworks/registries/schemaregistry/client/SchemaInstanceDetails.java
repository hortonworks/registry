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
package com.hortonworks.registries.schemaregistry.client;

import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SchemaVersion;

import java.io.Serializable;

/**
 * This class is about representing detailed information about a version of the schema.
 */
public class SchemaInstanceDetails implements Serializable {

    /**
     * Description about the schema.
     */
    private String schemaDescription;

    /**
     * Compatibility to be supported for all versions of this evolving schema.
     */
    private SchemaProvider.Compatibility compatibility;

    private SchemaVersion schemaVersion;

    @SuppressWarnings("unused")
    private SchemaInstanceDetails() { /** Private constructor for Jackson JSON mapping */}

    public SchemaInstanceDetails(SchemaVersion schemaVersion) {
        this(null, null, schemaVersion);
    }

    public SchemaInstanceDetails(String schemaDescription, SchemaProvider.Compatibility compatibility, SchemaVersion schemaVersion) {
        this.schemaDescription = schemaDescription;
        this.compatibility = compatibility;
        this.schemaVersion = schemaVersion;
    }

    public String getSchemaDescription() {
        return schemaDescription;
    }

    public SchemaProvider.Compatibility getCompatibility() {
        return compatibility;
    }

    public SchemaVersion getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public String toString() {
        return "SchemaInstanceDetails{" +
                "schemaDescription='" + schemaDescription + '\'' +
                ", compatibility=" + compatibility +
                ", schemaVersion=" + schemaVersion +
                '}';
    }
}
