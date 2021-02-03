/*
 * Copyright 2017-2021 Cloudera, Inc.
 *
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
package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;

public class AbstractSnapshotDeserializerTest {

    @Test
    public void getBooleanValueCanReadStringValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", "true");
        
        //when
        Boolean actual = underTest.getBooleanValue(config, "some key", false);
        
        //then
        Assert.assertThat(actual, is(true));
    }

    @Test
    public void getBooleanValueCanReadBooleanValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Boolean> config = new HashMap<>();
        config.put("some key", true);

        //when
        Boolean actual = underTest.getBooleanValue(config, "some key", false);

        //then
        Assert.assertThat(actual, is(true));
    }

    @Test
    public void getBooleanValueCanReadDefaultValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Boolean> config = new HashMap<>();
        config.put("some key", null);

        //when
        Boolean actual = underTest.getBooleanValue(config, "some key", false);

        //then
        Assert.assertThat(actual, is(false));
    }

    @Test
    public void getIntegerValueCanReadIntegerValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Integer> config = new HashMap<>();
        config.put("some key", 4);

        //when
        Integer actual = underTest.getIntegerValue(config, "some key", 99);

        //then
        Assert.assertThat(actual, is(4));
    }

    @Test
    public void getIntegerValueCanReadStringValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", "4");

        //when
        Integer actual = underTest.getIntegerValue(config, "some key", 99);

        //then
        Assert.assertThat(actual, is(4));
    }

    @Test
    public void getIntegerValueCanReadDefaultValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Integer> config = new HashMap<>();
        config.put("some key", null);

        //when
        Integer actual = underTest.getIntegerValue(config, "some key", 99);

        //then
        Assert.assertThat(actual, is(99));
    }

    @Test
    public void getLongValueCanReadLongValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Long> config = new HashMap<>();
        config.put("some key", 673L);

        //when
        Long actual = underTest.getLongValue(config, "some key", 99L);

        //then
        Assert.assertThat(actual, is(673L));
    }

    @Test
    public void getLongValueCanReadStringValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", "673");

        //when
        Long actual = underTest.getLongValue(config, "some key", 99L);

        //then
        Assert.assertThat(actual, is(673L));
    }

    @Test
    public void getLongValueCanReadDefaultValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", null);

        //when
        Long actual = underTest.getLongValue(config, "some key", 99L);

        //then
        Assert.assertThat(actual, is(99L));
    }
    
    class SnapshotDeserializer extends AbstractSnapshotDeserializer {

        @Override
        protected Object getParsedSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
            return null;
        }

        @Override
        protected Object doDeserialize(Object input, byte protocolId, SchemaMetadata schemaMetadata, 
                                       Integer writerSchemaVersion, Integer readerSchemaVersion) throws SerDesException {
            return null;
        }

        @Override
        protected byte retrieveProtocolId(Object input) throws SerDesException {
            return 0;
        }

        @Override
        protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, Object input) throws SerDesException {
            return null;
        }

        @Override
        public Object deserialize(Object input, Object readerSchemaInfo) throws SerDesException {
            return null;
        }
    }
}