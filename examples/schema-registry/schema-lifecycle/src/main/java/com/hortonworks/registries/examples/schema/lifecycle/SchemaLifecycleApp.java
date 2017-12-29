/*
 * Copyright 2017 Hortonworks.
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

package com.hortonworks.registries.examples.schema.lifecycle;

import com.hortonworks.registries.schemaregistry.SchemaBranchStorable;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;

public class SchemaLifecycleApp {

    private static LocalSchemaRegistryServer LOCAL_SCHEMA_REGISTRY_SERVER = null;

    public static void main(String[] args) throws Exception {
        startUp();
    }

    public static void startUp() throws Exception {
        LOCAL_SCHEMA_REGISTRY_SERVER = new LocalSchemaRegistryServer(SchemaLifecycleApp.class.getClassLoader().getResource("registry-lifecycle-example.yaml").getPath());
        LOCAL_SCHEMA_REGISTRY_SERVER.start();
    }

}
