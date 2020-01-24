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
package com.hortonworks.registries.schemaregistry.serdes;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * This class allows users to customize how schema version identifiers and actual payload are sent/received as part
 * of ser/des.
 */
public interface SerDesProtocolHandler {

    /**
     * @return protocol id for this handler.
     */
    Byte getProtocolId();

    /**
     * Serializes protocol id and schema version related information into the given output stream.
     *
     * @param outputStream    output stream
     * @param schemaIdVersion schema version info to be serialized
     */
    void handleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion) throws SerDesException;

    /**
     * Deserializes schema version related information from the given input stream.
     *
     * @param inputStream input stream
     * @return {@link SchemaIdVersion} instenace created from deserializing respective information from given input stream.
     */
    SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) throws SerDesException;

    /**
     * Handles serialization of input into given output stream
     *
     * @param outputStream output stream
     * @param input        object to be serialized
     */
    void handlePayloadSerialization(OutputStream outputStream, Object input) throws SerDesException;

    /**
     * Handles deserialization of given input stream and returns the deserialized Object.
     *
     * @param inputStream input stream
     * @param context     any context required for deserialization.
     * @return returns the deserialized Object.
     */
    Object handlePayloadDeserialization(InputStream inputStream, Map<String, Object> context) throws SerDesException;

}
