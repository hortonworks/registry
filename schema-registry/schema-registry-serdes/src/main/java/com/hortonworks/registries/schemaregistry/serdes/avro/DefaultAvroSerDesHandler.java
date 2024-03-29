/*
 * Copyright 2016-2019 Cloudera, Inc.
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

import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of serializing and deserializing avro payloads.
 */
public class DefaultAvroSerDesHandler implements AvroSerDesHandler {
    private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

    @Override
    public void handlePayloadSerialization(OutputStream outputStream, Object input) {
        try {
            Schema schema = AvroUtils.computeSchema(input);
            Schema.Type schemaType = schema.getType();
            if (Schema.Type.BYTES.equals(schemaType)) {
                // incase of byte arrays, no need to go through avro as there is not much to optimize and avro is expecting
                // the payload to be ByteBuffer instead of a byte array
                outputStream.write((byte[]) input);
            } else if (Schema.Type.STRING.equals(schemaType)) {
                // get UTF-8 bytes and directly send those over instead of using avro.
                outputStream.write(input.toString().getBytes("UTF-8"));
            } else {
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                DatumWriter<Object> writer;
                boolean isSpecificRecord = input instanceof SpecificRecord;
                if (isSpecificRecord) {
                    writer = new SpecificDatumWriter<>(schema);
                } else {
                    writer = new GenericDatumWriter<>(schema);
                }

                writer.write(input, encoder);
                encoder.flush();
            }
        } catch (IOException e) {
            throw new AvroRetryableException(e);
        } catch (RuntimeException e) {
            throw new AvroException(e);
        }
    }

    @Override
    public Object handlePayloadDeserialization(InputStream payloadInputStream,
                                               Schema writerSchema,
                                               Schema readerSchema,
                                               boolean useSpecificAvroReader) {
        Object deserializedObj;
        Schema.Type writerSchemaType = writerSchema.getType();
        try {
            if (Schema.Type.BYTES.equals(writerSchemaType)) {
                // serializer writes byte array directly without going through avro encoder layers.
                deserializedObj = IOUtils.toByteArray(payloadInputStream);
            } else if (Schema.Type.STRING.equals(writerSchemaType)) {
                // generate UTF-8 string object from the received bytes.
                deserializedObj = new String(IOUtils.toByteArray(payloadInputStream), AvroUtils.UTF_8);
            } else {
                DatumReader datumReader = getDatumReader(writerSchema, readerSchema, useSpecificAvroReader);
                deserializedObj = datumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
            }
        } catch (IOException e) {
            throw new AvroRetryableException(e);
        } catch (Exception e) {
            throw new AvroException(e);
        }
        return deserializedObj;
    }

    private DatumReader getDatumReader(Schema writerSchema, Schema readerSchema, boolean useSpecificAvroReader) {
        if (useSpecificAvroReader) {
            if (readerSchema == null) {
                readerSchema = this.getReaderSchema(writerSchema);
            }

            return new SpecificDatumReader(writerSchema, readerSchema);
        } else {
            return readerSchema == null ? new GenericDatumReader(writerSchema) : new GenericDatumReader(writerSchema, readerSchema);
        }
    }

    private Schema getReaderSchema(Schema writerSchema) {
        Schema readerSchema = this.readerSchemaCache.get(writerSchema.getFullName());
        if (readerSchema == null) {
            Class readerClass = SpecificData.get().getClass(writerSchema);
            if (readerClass == null) {
                throw new AvroException("Could not find class " + writerSchema.getFullName() + 
                        " specified in writer\'s schema whilst finding reader\'s schema for a SpecificRecord.");
            }
            try {
                readerSchema = ((SpecificRecord) readerClass.newInstance()).getSchema();
            } catch (InstantiationException e) {
                throw new AvroException(writerSchema.getFullName() + " specified by the " + 
                        "writers schema could not be instantiated to find the readers schema.");
            } catch (IllegalAccessException e) {
                throw new AvroException(writerSchema.getFullName() + " specified by the " + 
                        "writers schema is not allowed to be instantiated to find the readers schema.");
            }

            this.readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
        }

        return readerSchema;
    }
}
