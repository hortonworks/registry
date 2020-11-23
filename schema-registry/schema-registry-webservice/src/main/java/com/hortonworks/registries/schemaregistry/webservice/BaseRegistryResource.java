/**
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.webservice;

import com.google.common.base.Preconditions;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseRegistryResource {

    final ISchemaRegistry schemaRegistry;

    // Hack: Adding number in front of sections to get the ordering in generated swagger documentation correct
    static final String OPERATION_GROUP_SCHEMA = "1. Schema";
    static final String OPERATION_GROUP_SERDE = "2. Serializer/Deserializer";
    static final String OPERATION_GROUP_OTHER = "3. Other";



    BaseRegistryResource(ISchemaRegistry schemaRegistry) {
        Preconditions.checkNotNull(schemaRegistry, "SchemaRegistry can not be null");

        this.schemaRegistry = schemaRegistry;
    }

    static void checkValueAsNullOrEmpty(String name, String value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Parameter " + name + " is null");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Parameter " + name + " is empty");
        }
    }
    
}
