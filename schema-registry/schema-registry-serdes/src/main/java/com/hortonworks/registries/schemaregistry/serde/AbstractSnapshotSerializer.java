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
package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;

/**
 * This class implements {@link SnapshotSerializer} and internally creates schema registry client to connect to the
 * target schema registry.
 *
 * Extensions of this class need to implement below methods.
 * <ul>
 *    <li>{@link #doSerialize(Object, SchemaIdVersion)}</li>
 *    <li>{@link #getSchemaText(Object)}</li>
 * </ul>
 */
public abstract class AbstractSnapshotSerializer<I, O> extends AbstractSerDes implements SnapshotSerializer<I, O, SchemaMetadata> {

    public AbstractSnapshotSerializer() {
    }

    public AbstractSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    @Override
    public final O serialize(I input, SchemaMetadata schemaMetadata) throws SerDesException {
        ensureInitialized();

        // compute schema based on input object
        String schema = getSchemaText(input);

        // register that schema and get the version
        try {
            SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata, 
                    new SchemaVersion(schema, "Schema registered by serializer:" + this.getClass()));
            // write the version and given object to the output
            return doSerialize(input, schemaIdVersion);
        } catch (SchemaNotFoundException | IncompatibleSchemaException | InvalidSchemaException | SchemaBranchNotFoundException e) {
            throw new RegistryException(e);
        }
    }

    /**
     * Returns textual representation of the schema for the given {@code input} payload.
     * @param input input payload
     */
    protected abstract String getSchemaText(I input);

    /**
     * Returns the serialized object (which can be byte array or inputstream or any other object) which may contain all
     * the required information for deserializer to deserialize into the given {@code input}.
     *
     * @param input input object to be serialized
     * @param schemaIdVersion schema version info of the given input
     * @throws SerDesException when any ser/des Exception occurs
     */
    protected abstract O doSerialize(I input, SchemaIdVersion schemaIdVersion) throws SerDesException;

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("init should be invoked before invoking serialize operation");
        }

        if (closed) {
            throw new IllegalStateException("This serializer is already closed");
        }
    }

}
