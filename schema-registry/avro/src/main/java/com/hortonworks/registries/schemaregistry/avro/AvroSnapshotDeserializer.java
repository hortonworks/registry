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
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDeException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
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
    public Object deserialize(InputStream payloadInputStream, SchemaMetadataKey writerSchemaMetadataKey, Schema readerSchema) throws SerDeException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        try {
            payloadInputStream.read(byteBuffer.array());
        } catch (IOException e) {
            throw new SerDeException(e);
        }
        int version = byteBuffer.getInt();

        SchemaInfo writerSchemaInfo = schemaRegistryClient.getSchema(new SchemaKey(writerSchemaMetadataKey, version));
        if (writerSchemaInfo == null) {
            throw new SerDeException("No schema exists with metadata-key: " + writerSchemaMetadataKey + " and version: " + version);
        }

        String schemaText = writerSchemaInfo.getSchemaText();
        Schema writerSchema = new Schema.Parser().parse(schemaText);
        GenericDatumReader genericDatumReader = readerSchema != null ? new GenericDatumReader(writerSchema, readerSchema) : new GenericDatumReader(writerSchema);
        try {
            return genericDatumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
        } catch (IOException e) {
            throw new SerDeException(e);
        }

    }

    @Override
    public void close() throws Exception {
        schemaRegistryClient.close();
    }
}
