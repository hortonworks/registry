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

import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * SnapShotDeserializer implementation which deserializes schema id and version from received payload
 */
public abstract class AbstractSnapshotDeserializer<O> implements SnapshotDeserializer<O, SchemaInfo> {
    private final SchemaRegistryClient schemaRegistryClient;

    public AbstractSnapshotDeserializer(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public final O deserialize(InputStream payloadInputStream, SchemaInfo readerSchema) throws SerDeException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(12);
        try {
            payloadInputStream.read(byteBuffer.array());
        } catch (IOException e) {
            throw new SerDeException(e);
        }
        long id = byteBuffer.getLong();
        int version = byteBuffer.getInt();
        SchemaInfo writerSchema = schemaRegistryClient.getSchema(getSchemaKey(id, version));

        return doDeserialize(payloadInputStream, writerSchema, readerSchema);
    }

    protected abstract SchemaKey getSchemaKey(Long id, Integer version);

    protected abstract O doDeserialize(InputStream payloadInputStream, SchemaInfo writerSchema, SchemaInfo readerSchema);
}
