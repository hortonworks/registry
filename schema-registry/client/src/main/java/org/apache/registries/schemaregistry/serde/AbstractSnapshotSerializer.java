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
package org.apache.registries.schemaregistry.serde;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import org.apache.registries.schemaregistry.errors.IncompatibleSchemaException;
import org.apache.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.SchemaVersion;
import org.apache.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.Map;

/**
 *
 */
public abstract class AbstractSnapshotSerializer<I, O> implements SnapshotSerializer<I, O, SchemaMetadata> {
    private SchemaRegistryClient schemaRegistryClient;

    public AbstractSnapshotSerializer() {
    }

    @Override
    public void init(Map<String, ?> config) {
        schemaRegistryClient = new SchemaRegistryClient(config);
    }

    @Override
    public final O serialize(I input, SchemaMetadata schemaMetadata) throws SerDesException {

        // compute schema based on input object
        String schema = getSchemaText(input);

        // register that schema and get the version
        try {
            SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Schema registered by serializer:" + this.getClass()));
            // write the version and given object to the output
            return doSerialize(input, schemaIdVersion);
        } catch (InvalidSchemaException | IncompatibleSchemaException | SchemaNotFoundException e) {
            throw new SerDesException(e);
        }
    }

    protected abstract String getSchemaText(I input);

    protected abstract O doSerialize(I input, SchemaIdVersion schemaIdVersion) throws SerDesException;

    @Override
    public void close() throws Exception {
        schemaRegistryClient.close();
    }
}
