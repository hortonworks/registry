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
package com.hortonworks.registries.schemaregistry;

import java.io.Serializable;

/**
 * This can be used for adding details about versioned instance of a schema.
 * TODO: needs better description. Why is it called VersionedSchema?
 */
public class VersionedSchema implements Serializable {
    private String description;
    private String schemaText;

    @SuppressWarnings("unused")
    private VersionedSchema() { /* Private constructor for Jackson JSON mapping */ }

    public VersionedSchema(String schemaText, String description) {
        this.description = description;
        this.schemaText = schemaText;
    }

    public String getDescription() {
        return description;
    }

    public String getSchemaText() {
        return schemaText;
    }
}
