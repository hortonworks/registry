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
package com.hortonworks.registries.schemaregistry.avro.serdes;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;

/**
 *
 */
public class AvroSerDesInitClosureTest {
    private SchemaMetadata schemaMetadata =
            new SchemaMetadata.Builder("foo")
                    .type("avro")
                    .schemaGroup("bar")
                    .evolve(true)
                    .build();

    @Test(expected = IllegalStateException.class)
    public void testSerWihtoutInit() throws Exception {
        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        serializer.serialize(new Object(), schemaMetadata);
    }

    @Test(expected = IllegalStateException.class)
    public void testSerClosedInit() throws Exception {
        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        serializer.init(Collections.emptyMap());
        serializer.close();

        serializer.init(Collections.emptyMap());
    }

    @Test(expected = IllegalStateException.class)
    public void testSerClosedSer() throws Exception {
        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        serializer.init(Collections.emptyMap());
        serializer.close();

        serializer.serialize(new Object(), schemaMetadata);
    }


    @Test(expected = IllegalStateException.class)
    public void testDeserWihtoutInit() throws Exception {
        AvroSnapshotDeserializer deserializer = new AvroSnapshotDeserializer();
        deserializer.deserialize(new ByteArrayInputStream(new byte[]{}), 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testDeserClosedInit() throws Exception {
        AvroSnapshotDeserializer deserializer = new AvroSnapshotDeserializer();
        deserializer.init(Collections.emptyMap());
        deserializer.close();

        deserializer.init(Collections.emptyMap());
    }

    @Test(expected = IllegalStateException.class)
    public void testDeserClosedSer() throws Exception {
        AvroSnapshotDeserializer deserializer = new AvroSnapshotDeserializer();
        deserializer.init(Collections.emptyMap());
        deserializer.close();

        deserializer.deserialize(new ByteArrayInputStream(new byte[]{}), 1);
    }

}
