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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class AvroSnapshotDeserializer implements SnapshotDeserializer<InputStream, Object, SchemaMetadataKey, Schema> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSnapshotDeserializer.class);

    private SchemaRegistryClient schemaRegistryClient;

    @Override
    public void init(Map<String, ?> config) {
        schemaRegistryClient = new SchemaRegistryClient(config);
    }

    @Override
    public Object deserialize(InputStream payloadInputStream, SchemaMetadataKey writerSchemaMetadataKey, Schema readerSchema) throws SerDesException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        try {
            payloadInputStream.read(byteBuffer.array());
        } catch (IOException e) {
            throw new SerDesException(e);
        }
        int version = byteBuffer.getInt();

        SchemaInfo writerSchemaInfo = schemaRegistryClient.getSchema(new SchemaKey(writerSchemaMetadataKey, version));
        if (writerSchemaInfo == null) {
            throw new SerDesException("No schema exists with metadata-key: " + writerSchemaMetadataKey + " and version: " + version);
        }

        // todo have cache to avoid parsing schema every time
        String schemaText = writerSchemaInfo.getSchemaText();
        Schema writerSchema = new Schema.Parser().parse(schemaText);
        Object deserializedObj = null;
        try {
            Schema.Type writerSchemaType = writerSchema.getType();
            if (Schema.Type.BYTES.equals(writerSchemaType)) {
                // serializer writes byte array directly without going through avro decoder layers.
                deserializedObj = IOUtils.toByteArray(payloadInputStream);
            } else if (Schema.Type.STRING.equals(writerSchemaType)) {
                // generate UTF-8 string object from the received bytes.
                deserializedObj = new String(IOUtils.toByteArray(payloadInputStream), AvroUtils.UTF_8);
            } else {
                GenericDatumReader genericDatumReader = readerSchema != null ? new GenericDatumReader(writerSchema, readerSchema) : new GenericDatumReader(writerSchema);
                deserializedObj = genericDatumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
                // String type's values are not always returned as String objects but with internal Avro representations for UTF-8.
                if (Schema.Type.STRING == writerSchemaType) {
                    deserializedObj = deserializedObj.toString();
                }
            }
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        return deserializedObj;
    }

    @Override
    public void close() throws Exception {
        schemaRegistryClient.close();
    }
}
