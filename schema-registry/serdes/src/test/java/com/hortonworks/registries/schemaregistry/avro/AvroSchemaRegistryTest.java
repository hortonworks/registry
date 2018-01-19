/**
 * Copyright 2016 Hortonworks.
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
 **/
package com.hortonworks.registries.schemaregistry.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.HAServerNotificationManager;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class AvroSchemaRegistryTest {

    private static final String SCHEMA_GROUP = "test-group";
    private static final String INVALID_SCHEMA_METADATA_KEY = "invalid-schema" + System.currentTimeMillis();

    private DefaultSchemaRegistry schemaRegistry;

    @Rule
    public TestName TEST_NAME_RULE = new TestName();

    @Before
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device-compat.avsc");
        schemaName = "org.hwx.schemas.test-schema." + UUID.randomUUID();
        StorageManager storageManager = new InMemoryStorageManager();
        Collection<Map<String, Object>> schemaProvidersConfig = Collections.singleton(Collections.singletonMap("providerClass", AvroSchemaProvider.class.getName()));
        schemaRegistry = new DefaultSchemaRegistry(storageManager, null, schemaProvidersConfig, new HAServerNotificationManager());
        schemaRegistry.init(Collections.<String, Object>emptyMap());
    }

    protected String schema1;
    protected String schema2;
    protected String schemaName;

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSchemaRegistryTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testSchemaMetadataOps() throws Exception {
        for (SchemaCompatibility schemaCompatibility : SchemaCompatibility.values()) {
            SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("compatibility-" + schemaCompatibility)
                    .type(AvroSchemaProvider.TYPE)
                    .description("devices schema")
                    .compatibility(schemaCompatibility)
                    .schemaGroup(SCHEMA_GROUP).build();

            Long schemaMetadataId = schemaRegistry.registerSchemaMetadata(schemaMetadata);
            SchemaMetadata schemaMetadataReturned = schemaRegistry.getSchemaMetadataInfo(schemaMetadataId).getSchemaMetadata();
            Assert.assertEquals(schemaMetadata, schemaMetadataReturned);
        }
    }

    @Test
    public void testRegistrySchemaOps() throws Exception {
        SchemaCompatibility compatibility = SchemaCompatibility.BOTH;
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(schemaName)
                .type(AvroSchemaProvider.TYPE)
                .description("devices schema")
                .compatibility(compatibility)
                .schemaGroup(SCHEMA_GROUP).build();

        Long schemaMetadataId = schemaRegistry.registerSchemaMetadata(schemaMetadata);
        SchemaMetadata schemaMetadataReturned = schemaRegistry.getSchemaMetadataInfo(schemaMetadataId).getSchemaMetadata();
        Assert.assertEquals(schemaMetadata, schemaMetadataReturned);

        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadata,
                                                     new SchemaVersion(schema1, "initial version of the schema"))
                                   .getVersion();
        Integer v2 = schemaRegistry.addSchemaVersion(schemaName,
                                                     new SchemaVersion(schema2, "second version of the the schema"))
                                   .getVersion();
        Assert.assertTrue(v2 == v1 + 1);

        Collection<SchemaVersionInfo> allSchemaVersions = schemaRegistry.getAllVersions(schemaName);
        Assert.assertTrue(allSchemaVersions.size() == 2);

        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaName);
        Assert.assertEquals(schemaMetadata, schemaMetadataInfo.getSchemaMetadata());

        Integer schemaVersion = schemaRegistry.getSchemaVersionInfo(schemaName, schema1).getVersion();
        Assert.assertEquals(v1, schemaVersion);

        SchemaVersionInfo schemaVersionInfo1 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v1));
        Assert.assertEquals(schemaVersionInfo1.getSchemaText(), schema1);

        SchemaVersionInfo schemaVersionInfo2 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2));
        Assert.assertEquals(schemaVersionInfo2.getSchemaText(), schema2);

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        Integer version = schemaRegistry.addSchemaVersion(schemaMetadata,
                                                          new SchemaVersion(schema1, "already added schema"))
                                        .getVersion();
        Assert.assertEquals(version, v1);

        //aggregate apis
        AggregatedSchemaMetadataInfo aggregatedSchemaMetadata = schemaRegistry.getAggregatedSchemaMetadataInfo(schemaName);
        Assert.assertEquals(allSchemaVersions.size(), aggregatedSchemaMetadata.getSchemaBranches().iterator().next().getSchemaVersionInfos().size());
        Assert.assertTrue(aggregatedSchemaMetadata.getSerDesInfos().isEmpty());

        Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataCollection = schemaRegistry.findAggregatedSchemaMetadata(Collections.emptyMap());
        Assert.assertEquals(1, aggregatedSchemaMetadataCollection.size());

        // Serializing and deserializing AggregatedSchemaMetadataInfo should not throw any errors

        String aggregateSchemaMetadataStr = new ObjectMapper().writeValueAsString(aggregatedSchemaMetadataCollection);

        Collection<AggregatedSchemaMetadataInfo> returnedResult =
                new ObjectMapper().readValue(aggregateSchemaMetadataStr,
                        new TypeReference<Collection<AggregatedSchemaMetadataInfo>>() {});
    }

    @Test
    public void testNonExistingSchemaMetadata() {
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(INVALID_SCHEMA_METADATA_KEY);
        Assert.assertNull(schemaMetadataInfo);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testAddVersionToNonExistingSchema() throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {
        schemaRegistry.addSchemaVersion(INVALID_SCHEMA_METADATA_KEY, new SchemaVersion("foo", "dummy"));
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidSchema() throws Exception {
        String schema = "--- random invalid schema ---" + new Date();

        SchemaMetadata schemaMetadataInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadataInfo,
                                                     new SchemaVersion(schema, "Initial version of the schema"))
                                   .getVersion();
    }

    @Test(expected = InvalidSchemaException.class)
    public void testSchemaTextAsNull() throws Exception {

        SchemaMetadata schemaMetadataInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadataInfo,
                                                     new SchemaVersion(null, "Initial version of the schema"))
                                   .getVersion();
    }

    @Test(expected = InvalidSchemaException.class)
    public void testSchemaTextAsEmpty() throws Exception {

        SchemaMetadata schemaMetadataInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadataInfo,
                                                     new SchemaVersion("", "Initial version of the schema"))
                                   .getVersion();
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = getSchema("/device.avsc");
        String incompatSchema = getSchema("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadata,
                                                     new SchemaVersion(schema, "Initial version of the schema"))
                                   .getVersion();

        // adding a new version of the schema
        Integer v2 = schemaRegistry.addSchemaVersion(schemaMetadata,
                                                     new SchemaVersion(incompatSchema, "second version"))
                                   .getVersion();
    }

    private SchemaMetadata createSchemaInfo(String testName, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(testName + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(testName + "-group")
                .description("Schema for " + testName)
                .compatibility(compatibility)
                .build();
    }

}