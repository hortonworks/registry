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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hortonworks.registries.schemaregistry.serde.push;

import java.io.InputStream;

/**
 * This class can be implemented to handle events when it is registered with {@link PushDeserializer#deserialize(InputStream, Object, PushDeserializerHandler)}
 */
public interface PushDeserializerHandler<F> {

    /**
     * Indicates deserialization of the given payload is started.
     */
    public void startDeserialize();

    /**
     * Indicates the deserialization of the given payload is finished.
     */
    public void endDeserialize();

    /**
     * Indicates the deserialization of a new record is started.
     */
    public void startRecord();

    /**
     * Indicates the current record is finished
     */
    public void endRecord();

    /**
     * Indicates the deserialization of the given field is started
     *
     * @param startField
     */
    public void startField(F startField);

    /**
     * Callback to receive the currently deserialized value.
     *
     * @param value
     */
    public void readFieldValue(Object value);

    /**
     * Indicates the deserialization of the given field is finished.
     *
     * @param endField
     */
    public void endField(F endField);

    /**
     * Callback to receive current field and value
     *
     * @param field
     * @param value
     */
    public void currentFieldValue(F field, Object value);

}
