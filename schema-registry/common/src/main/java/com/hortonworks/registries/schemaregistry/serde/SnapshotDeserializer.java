/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.schemaregistry.Resourceable;

/**
 * Deserializer interface for deserializing InputStream into output {@code O} according to the Schema {@code S}.
 * <p>
 *
 * @param <I>  Input type of the payload
 * @param <O>  Output type of the deserialized content.
 * @param <WS> Writer schema information.
 * @param <RS> Reader schema information.
 */
public interface SnapshotDeserializer<I, O, WS, RS> extends Resourceable {

    /**
     * This property represents the version of a reader schema to be used in deserialization.
     */
    String READER_VERSION = "schemaregistry.reader.schema.version";

    /**
     * Returns output {@code O} after deserializing the given {@code input} according to the writer schema {@code WS} and
     * it may be projected if reader schema {@code RS} is given.
     *
     * @param input
     * @param writerSchemaInfo
     * @param readerSchemaInfo
     * @return O output
     */
    O deserialize(I input, WS writerSchemaInfo, RS readerSchemaInfo) throws SerDesException;

}
