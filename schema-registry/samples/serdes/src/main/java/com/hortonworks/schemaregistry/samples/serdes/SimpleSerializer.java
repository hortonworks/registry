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
package com.hortonworks.schemaregistry.samples.serdes;

import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * This is a very simple serializer, it does not really connect to schema-registry and register schemas. This
 * is used to demonstrate how this serializer can be uploaded/retrieved from schema registry.
 * <p>
 * You can look at {@link com.hortonworks.registries.schemaregistry.avro.AvroSnapshotSerializer} implementation to look
 * at how schema registry client can be used to register writer schemas which are used while deserializing messages from the respective {@link com.hortonworks.registries.schemaregistry.avro.AvroSnapshotDeserializer}.
 */
public class SimpleSerializer implements SnapshotSerializer<Object, byte[], SchemaInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSerializer.class);

    @Override
    public byte[] serialize(Object input, SchemaInfo schemaInfo) throws SerDesException {
        LOG.info("Received payload: [{}] with schema info: [{}]", input, schemaInfo);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public void init(Map<String, ?> config) {

    }

    @Override
    public void close() throws Exception {

    }
}
