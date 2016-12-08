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
package com.hortonworks.registries.schemaregistry.streams;

import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;

/**
 *
 */
public class StreamsSchemaProviderTest {
    private static final Logger LOG = LoggerFactory.getLogger(StreamsSchemaProviderTest.class);

    @Test
    public void testStreamsSchema() throws Exception {
        StreamsSchemaProvider streamsSchemaProvider = new StreamsSchemaProvider();
        try (InputStream trucksSchemaStream = StreamsSchemaProviderTest.class.getResourceAsStream("/streams/trucks.stsc");) {
            List<SchemaFieldInfo> schemaFieldInfos = streamsSchemaProvider.generateFields(IOUtils.toString(trucksSchemaStream));
            LOG.info("schemaFieldInfos = " + schemaFieldInfos);
            Assert.assertEquals(11, schemaFieldInfos.size());
        }
    }
}
