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
package com.hortonworks.registries.schemaregistry.avro;

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
public class AvroSchemaProviderTest {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaProviderTest.class);

    @Test
    public void testAvroSchemas() throws Exception {
        AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        try(InputStream schemaStream = AvroSchemaProviderTest.class.getResourceAsStream("/avro/trucks.avsc") ;) {
            List<SchemaFieldInfo> schemaFieldInfos = avroSchemaProvider.generateFields(IOUtils.toString(schemaStream));
            LOG.info("generated schema fields [{}]", schemaFieldInfos);
            Assert.assertEquals(11, schemaFieldInfos.size());
        }
    }
}
