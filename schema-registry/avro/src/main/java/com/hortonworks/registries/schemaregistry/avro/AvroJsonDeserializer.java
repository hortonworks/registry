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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.hortonworks.registries.schemaregistry.serde.SerDeException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Deserializes the JSON payload InputStream to desired output {@code O} with the given Avro schema
 *
 * @param <O> Type of deserialized output
 */
public class AvroJsonDeserializer<O> implements SnapshotDeserializer<O, Schema> {

    public AvroJsonDeserializer() {
    }

    @Override
    public void init(Map<String, Object> config) {

    }

    @Override
    public O deserialize(InputStream inputStream, Schema schema) throws SerDeException {
        Preconditions.checkNotNull(inputStream, "InputStream can not be null");
        Preconditions.checkNotNull(schema, "schema can not be null");

        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            Encoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            String payload = new String(outputStream.toByteArray(), "UTF-8");
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(payload, new TypeReference<O>() {});
        } catch (IOException e) {
            throw new SerDeException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
