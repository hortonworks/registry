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

import org.apache.avro.Schema;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface for serializing and deserializing avro payloads.
 */
public interface AvroSerDesHandler {

    void handlePayloadSerialization(OutputStream outputStream, Object input, boolean logicalTypeConversionEnabled);

    Object handlePayloadDeserialization(InputStream payloadInputStream,
                                        Schema writerSchema,
                                        Schema readerSchema,
                                        boolean useSpecificAvroReader,
                                        boolean logicalTypeConversionEnabled);
}
