/*
 * Copyright 2016-2023 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.serdes.json.kafka;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.MockSchemaRegistryClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;

public class KafkaJsonSerializerTest {
  private ISchemaRegistryClient mockClient;

  @BeforeEach
  void setup() {
    mockClient = new MockSchemaRegistryClient();
  }

  @AfterEach
  void teardown() throws Exception {
    mockClient.close();
  }

  @Test
  void serializeNullWithoutPassthrough() {
    KafkaJsonSerializer serializer = new KafkaJsonSerializer(mockClient);
    Map<String, Object> config = new HashMap<>();
    serializer.configure(config, false);

    byte[] data = serializer.serialize("topic", null);

    assertNull(data);
  }
}
