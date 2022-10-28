/**
 * Copyright 2016-2022 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AbstractAvroSnapshotDeserializerTest {

    @Test
    public void canReadBooleanPropertiesFromKafkaConfigsStringValue() {
        //given
        AvroSnapshotDeserializer underTest = new AvroSnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("specific.avro.reader", "true");
        //when
        underTest.doInit(config);
        
        //then
        Assertions.assertTrue(underTest.isUseSpecificAvroReader());
    }
    
    @Test
    public void canReadBooleanPropertiesFromKafkaConfigsBooleanValue() {
        //given
        AvroSnapshotDeserializer underTest = new AvroSnapshotDeserializer();
        Map<String, Boolean> config = new HashMap<>();
        config.put("specific.avro.reader", true);
        //when
        underTest.doInit(config);

        //then
        Assertions.assertTrue(underTest.isUseSpecificAvroReader());
    }

    @Test
    public void canReadBooleanPropertiesFromKafkaConfigsDefaultValue() {
        //given
        AvroSnapshotDeserializer underTest = new AvroSnapshotDeserializer();
        Map<String, Boolean> config = new HashMap<>();
        config.put("specific.avro.reader", null);
        //when
        underTest.doInit(config);

        //then
        Assertions.assertFalse(underTest.isUseSpecificAvroReader());
    }
    
    class AvroSnapshotDeserializer extends AbstractAvroSnapshotDeserializer<String> {
        @Override
        protected Object doDeserialize(String input, byte protocolId, String schemaName, Integer writerSchemaVersion, Integer readerSchemaVersion) throws SerDesException {
            return null;
        }

        @Override
        protected byte retrieveProtocolId(String input) throws SerDesException {
            return 0;
        }

        @Override
        protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, String input) throws SerDesException {
            return null;
        }
    };
}
