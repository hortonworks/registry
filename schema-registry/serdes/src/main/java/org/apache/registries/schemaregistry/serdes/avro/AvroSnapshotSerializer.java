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
package org.apache.registries.schemaregistry.serdes.avro;

import org.apache.registries.schemaregistry.SchemaIdVersion;
import org.apache.registries.schemaregistry.avro.AvroSchemaProvider;
import org.apache.registries.schemaregistry.serde.AbstractSnapshotSerializer;
import org.apache.registries.schemaregistry.serde.SerDesException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class AvroSnapshotSerializer extends AbstractSnapshotSerializer<Object, byte[]> {

    public AvroSnapshotSerializer() {
    }

    protected byte[] doSerialize(Object input, SchemaIdVersion schemaIdVersion) throws SerDesException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            writeProtocolMsg(schemaIdVersion, byteArrayOutputStream);


            Schema schema = computeSchema(input);
            Schema.Type schemaType = schema.getType();
            if (Schema.Type.BYTES.equals(schemaType)) {
                // incase of byte arrays, no need to go through avro as there is not much to optimize and avro is expecting
                // the payload to be ByteBuffer instead of a byte array
                byteArrayOutputStream.write((byte[]) input);
            } else if (Schema.Type.STRING.equals(schemaType)) {
                // get UTF-8 bytes and directly send those over instead of usng avro.
                byteArrayOutputStream.write(input.toString().getBytes(AvroUtils.UTF_8));
            } else {
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
                DatumWriter<Object> writer;
                boolean isSpecificRecord = input instanceof SpecificRecord;
                byteArrayOutputStream.write(isSpecificRecord ? AvroUtils.SPECIFIC_RECORD : AvroUtils.GENERIC_RECORD);
                if (isSpecificRecord) {
                    writer = new SpecificDatumWriter<>(schema);
                } else {
                    writer = new GenericDatumWriter<>(schema);
                }

                writer.write(input, encoder);
                encoder.flush();
            }

            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new SerDesException(e);
        }
    }

    private void writeProtocolMsg(SchemaIdVersion schemaIdVersion, ByteArrayOutputStream byteArrayOutputStream) throws IOException {
        // it can be enhanced to have respective protocol handlers for different versions
        // first byte is protocol version/id.
        // protocol format:
        // 1 byte  : protocol version
        // 8 bytes : schema metadata Id
        // 4 bytes : schema version
        byteArrayOutputStream.write(ByteBuffer.allocate(13)
                                            .put(AvroSchemaProvider.CURRENT_PROTOCOL_VERSION)
                                            .putLong(schemaIdVersion.getSchemaMetadataId())
                                            .putInt(schemaIdVersion.getVersion()).array());
    }

    protected String getSchemaText(Object input) {
        Schema schema = computeSchema(input);
        return schema.toString();
    }

    private Schema computeSchema(Object input) {
        Schema schema = null;
        if (input instanceof GenericContainer) {
            schema = ((GenericContainer) input).getSchema();
        } else {
            schema = AvroUtils.getSchemaForPrimitives(input);
        }
        return schema;
    }

}
