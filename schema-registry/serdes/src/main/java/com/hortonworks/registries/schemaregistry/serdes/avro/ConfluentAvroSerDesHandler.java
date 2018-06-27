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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

/**
 * Confluent compatible implementation of serializing and deserializing avro payloads.
 */
public class ConfluentAvroSerDesHandler implements AvroSerDesHandler {

    @Override
    public void handlePayloadSerialization(OutputStream outputStream, Object input, DatumWriterProvider datumWriterProvider) {
        try {
            Schema schema = AvroUtils.computeSchema(input);
            if (input instanceof byte[]) {
                outputStream.write((byte[]) input);
            } else {
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                DatumWriter<Object> writer = datumWriterProvider.getDatumWriter(schema, input);
                writer.write(input, encoder);
                encoder.flush();
            }
        } catch (IOException e) {
            throw new AvroRetryableException("Error serializing Avro message", e);
        } catch (RuntimeException e) {
            // avro serialization can throw AvroRuntimeException, NullPointerException,
            // ClassCastException, etc
            throw new AvroException("Error serializing Avro message", e);
        }
    }

    @Override
    public Object handlePayloadDeserialization(InputStream payloadInputStream,
                                               Schema writerSchema,
                                               Schema readerSchema,
                                               DatumReaderProvider datumReaderProvider) {

        Object deserializedObj;
        try {
            if (Schema.Type.BYTES.equals(writerSchema.getType())) {
                // serializer writes byte array directly without going through avro encoder layers.
                deserializedObj = IOUtils.toByteArray(payloadInputStream);
            } else {
                DatumReader datumReader = datumReaderProvider.getDatumReader(writerSchema, readerSchema);
                deserializedObj = datumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
            }
        } catch (IOException e) {
            throw new AvroRetryableException("Error deserializing Avro message for id " + writerSchema, e);
        } catch (RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new AvroException("Error deserializing Avro message for id " + writerSchema, e);
        }
        return deserializedObj;
    }
}
