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
package com.hortonworks.registries.schemaregistry.serdes;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class allows users to customize how schema version identifiers are sent/received as part
 * of ser/des. This does not include how the actual payload is serialized or deserialized which is left to serializer
 * or deserializer implementation like {@link com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer}
 * and {@link com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer}.
 *
 */
public interface SchemaVersionProtocolHandler {

    /**
     * Serializes protocol id and schema version related information into the given output stream.
     * @param outputStream
     * @param schemaIdVersion
     * @throws IOException
     */
    void handleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion) throws IOException;

    /**
     * Deserializes schema version related information from the given input stream.
     * @param inputStream
     * @return {@link SchemaIdVersion} instenace created from deserializing respective information from given input stream.
     */
    SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream);

    /**
     * @return protocol id for this handler.
     */
    Byte getProtocolId();

}
