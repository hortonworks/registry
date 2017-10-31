/*
 * Copyright 2016 Hortonworks.
 * <p>
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
 */
package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * This is the default implementation of {@link AbstractAvroSnapshotDeserializer}.
 * <p>
 * <p>Common way to use this serializer implementation is like below as mentioned in {@link com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer}. </p>
 * <pre>{@code
 *     AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
 *     // initialize with given configuration
 *     serializer.init(config);
 *
 *     // this instance can be used for multiple serialization invocations
 *     serializer.serialize(input, schema);
 *
 *     // close it to release any resources held
 *     serializer.close();
 * }</pre>
 */
public class AvroSnapshotSerializer extends AbstractAvroSnapshotSerializer<byte[]> {

    public AvroSnapshotSerializer() {
    }

    public AvroSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    protected byte[] doSerialize(Object input, SchemaIdVersion schemaIdVersion) throws SerDesException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            serializeSchemaVersion(baos, schemaIdVersion);
            serializePayload(baos, input);

            return baos.toByteArray();
        } catch (IOException e) {
            throw new AvroRetryableException(e);
        }
    }

}
