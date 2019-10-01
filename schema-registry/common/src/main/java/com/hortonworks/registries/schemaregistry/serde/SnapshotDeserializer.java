/**
 * Copyright 2016-2019 Cloudera, Inc.
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
 * Deserializer interface for de-serializing input {@code I} into output {@code O} according to the Schema {@code S}.
 * <p>Common way to use this deserializer implementation is like below. </p>
 * <pre>{@code
 *     SnapshotDeserializer deserializer = ...
 *     // initialize with given configuration
 *     deserializer.init(config);
 *
 *     // this instance can be used for multiple serialization invocations
 *     deserializer.deserialize(input, readerSchemaInfo);
 *
 *     // close it to release any resources held
 *     deserializer.close();
 * }</pre>
 *
 * @param <I>  Input type of the payload
 * @param <O>  Output type of the de-serialized content.
 * @param <RS> Reader schema information.
 */
public interface SnapshotDeserializer<I, O, RS> extends Resourceable {

    /**
     * De-serializes the given {@code input} according to the writer schema {@code WS} and
     * projects it based on the reader schema {@code RS} (if not null).
     *
     * @param input input payload to be de-serialized
     * @param readerSchemaInfo schema information for reading/projection
     * @return O the de-serialized output
     * @throws SerDesException
     */
    O deserialize(I input, RS readerSchemaInfo) throws SerDesException;

    /**
     * De-serializes the given {@code input} according to the writer schema {@code WS}.
     *
     * @param input input payload to be de-serialized
     * @return O the de-serialized output
     * @throws SerDesException
     */
    default O deserialize(I input) throws SerDesException {
        return deserialize(input, null);
    }

}
