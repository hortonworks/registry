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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.serde.SerDeException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Serializes the given input {@code I} to byte[] or OutputStream using the given Avro schema.
 *
 * @param <I> type of input payload to be serialized
 */
public class AvroJsonSerializer<I> implements SnapshotSerializer<I, byte[], Schema> {

    @Override
    public void init(Map<String, Object> config) {

    }

    @Override
    public byte[] serialize(I input, Schema schema) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        serialize(input, outputStream, schema);
        return outputStream.toByteArray();
    }

    @Override
    public void serialize(I input, OutputStream outputStream, Schema schema) throws SerDeException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json =  mapper.writeValueAsString(input);

            GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new SerDeException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
