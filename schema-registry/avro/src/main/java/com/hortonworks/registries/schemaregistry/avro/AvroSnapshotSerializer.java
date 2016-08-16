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

import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.client.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDeException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class AvroSnapshotSerializer implements SnapshotSerializer<Object, byte[], SchemaMetadata> {

    public static final String TYPE = "AVRO";
    private SchemaRegistryClient schemaRegistryClient;

    public AvroSnapshotSerializer() {
    }

    @Override
    public void init(Map<String, Object> config) {
        schemaRegistryClient = new SchemaRegistryClient(config);
    }

    @Override
    public byte[] serialize(Object input, SchemaMetadata schemaMetadataInfo) throws SerDeException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serialize(input, baos, schemaMetadataInfo);

        return baos.toByteArray();
    }

    private SchemaMetadata createSchemaMetadata(SchemaMetadata schemaMetadata, org.apache.avro.Schema avroSchema) {
        return new SchemaMetadata(schemaMetadata.getName(), schemaMetadata.getDescription(),
                schemaMetadata.getDescription(), schemaMetadata.getCompatibility(), avroSchema.toString());
    }

    private org.apache.avro.Schema getSchema(Object input) {
        if(input instanceof GenericContainer) {
            return ((GenericContainer) input).getSchema();
        }
        
        throw new IllegalArgumentException("input is not an instance of GenericContainer");
    }

    @Override
    public void serialize(Object input, OutputStream outputStream, SchemaMetadata schemaMetadataInfo) throws SerDeException {
        org.apache.avro.Schema schema = getSchema(input);
        // register given schema
        SchemaKey schemaKey = schemaRegistryClient.registerSchema(createSchemaMetadata(schemaMetadataInfo, schema));
        try {
            // write schema id and version, both of them require 12 bytes (Long +Int)
            outputStream.write(ByteBuffer.allocate(12).putLong(schemaKey.getId()).putInt(schemaKey.getVersion()).array());

            // todo handle all cases
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
            DatumWriter<Object> writer;
            if (input instanceof SpecificRecord) {
                writer = new SpecificDatumWriter<>(schema);
            } else {
                writer = new GenericDatumWriter<>(schema);
            }
            writer.write(input, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new SerDeException(e);
        }

    }

    @Override
    public void close() throws Exception {

    }
}
