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
package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.client.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * SnapshotSerializer implementation which sends schema-id and version to the outputstream by taking {@link SchemaMetadata} instance.
 */
public abstract class AbstractSnapshotSerializer<O> implements SnapshotSerializer<InputStream, O, SchemaMetadata> {
    private SchemaRegistryClient schemaRegistryClient;

    protected AbstractSnapshotSerializer() {
    }

    @Override
    public void init(Map<String, Object> config) {
        schemaRegistryClient = new SchemaRegistryClient();
        schemaRegistryClient.init(config);
    }

    protected abstract void doSerialize(InputStream input, OutputStream outputStream, SchemaMetadata schemaMetadata) throws SerDeException;

    @Override
    public final O serialize(InputStream input, SchemaMetadata schemaMetadata) throws SerDeException {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public final void serialize(InputStream input, OutputStream outputStream, SchemaMetadata schemaMetadata) throws SerDeException {

        try {
            // register given schema
            SchemaKey schemaKey = schemaRegistryClient.registerSchema(schemaMetadata);
            // write schema id and version, both of them require 12 bytes (Long +Int)
            outputStream.write(ByteBuffer.allocate(12).putLong(schemaKey.getId()).putInt(schemaKey.getVersion()).array());
        } catch (IOException e) {
            throw new SerDeException(e);
        }

        doSerialize(input, outputStream, schemaMetadata);
    }

}
