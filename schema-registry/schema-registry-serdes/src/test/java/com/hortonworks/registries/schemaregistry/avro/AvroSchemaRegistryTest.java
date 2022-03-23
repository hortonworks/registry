/**
 * Copyright 2016-2019 Cloudera, Inc.
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
import com.hortonworks.registries.common.CompatibilityConfig;
import com.hortonworks.registries.common.ModuleDetailsConfiguration;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class AvroSchemaRegistryTest {

    private static final String SCHEMA_GROUP = "test-group";
    private static final String INVALID_SCHEMA_METADATA_KEY = "invalid-schema" + System.currentTimeMillis();

    private DefaultSchemaRegistry schemaRegistry;

    String displayName;

    @BeforeEach
    void init(TestInfo testInfo) {
        displayName = testInfo.getDisplayName();
    }

    @BeforeEach
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device-compat.avsc");
        schemaName = "org.hwx.schemas.test-schema." + UUID.randomUUID();
        StorageManager storageManager = new InMemoryStorageManager();
        Collection<Map<String, Object>> schemaProvidersConfig = Collections.singleton(Collections.singletonMap("providerClass", 
                AvroSchemaProvider.class.getName()));
        schemaRegistry = new DefaultSchemaRegistry(new ModuleDetailsConfiguration(), storageManager, null, schemaProvidersConfig,
                new SchemaLockManager(new NOOPTransactionManager()), new CompatibilityConfig());
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
            Assertions.assertEquals(schemaMetadata, schemaMetadataReturned);
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
        Assertions.assertEquals(schemaMetadata, schemaMetadataReturned);

        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadata,
                                                     new SchemaVersion(schema1, "initial version of the schema"))
                                   .getVersion();
        Integer v2 = schemaRegistry.addSchemaVersion(schemaName,
                                                     new SchemaVersion(schema2, "second version of the the schema"))
                                   .getVersion();
        Assertions.assertTrue(v2 == v1 + 1);

        Collection<SchemaVersionInfo> allSchemaVersions = schemaRegistry.getAllVersions(schemaName);
        Assertions.assertTrue(allSchemaVersions.size() == 2);

        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaName);
        Assertions.assertEquals(schemaMetadata, schemaMetadataInfo.getSchemaMetadata());

        Integer schemaVersion = schemaRegistry.getSchemaVersionInfo(schemaName, schema1).getVersion();
        Assertions.assertEquals(v1, schemaVersion);

        SchemaVersionInfo schemaVersionInfo1 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v1));
        Assertions.assertEquals(schema1, schemaVersionInfo1.getSchemaText());

        SchemaVersionInfo schemaVersionInfo2 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2));
        Assertions.assertEquals(schema2, schemaVersionInfo2.getSchemaText());

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        Integer version = schemaRegistry.addSchemaVersion(schemaMetadata,
                                                          new SchemaVersion(schema1, "already added schema"))
                                        .getVersion();
        Assertions.assertEquals(v1, version);

        //aggregate apis
        AggregatedSchemaMetadataInfo aggregatedSchemaMetadata = schemaRegistry.getAggregatedSchemaMetadataInfo(schemaName);
        Assertions.assertEquals(allSchemaVersions.size(), aggregatedSchemaMetadata.getSchemaBranches().iterator().next().getSchemaVersionInfos().size());
        Assertions.assertTrue(aggregatedSchemaMetadata.getSerDesInfos().isEmpty());

        Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataCollection = schemaRegistry.findAggregatedSchemaMetadata(Collections.emptyMap());
        Assertions.assertEquals(1, aggregatedSchemaMetadataCollection.size());

        // Serializing and deserializing AggregatedSchemaMetadataInfo should not throw any errors

        String aggregateSchemaMetadataStr = new ObjectMapper().writeValueAsString(aggregatedSchemaMetadataCollection);

        Collection<AggregatedSchemaMetadataInfo> returnedResult =
                new ObjectMapper().readValue(aggregateSchemaMetadataStr,
                        new TypeReference<Collection<AggregatedSchemaMetadataInfo>>() { });
    }

    @Test
    public void testNonExistingSchemaMetadata() {
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(INVALID_SCHEMA_METADATA_KEY);
        Assertions.assertNull(schemaMetadataInfo);
    }

    @Test
    public void testAddVersionToNonExistingSchema() {
        Assertions.assertThrows(SchemaNotFoundException.class, () -> schemaRegistry.addSchemaVersion(INVALID_SCHEMA_METADATA_KEY, new SchemaVersion("foo", "dummy")));
    }

    @Test
    public void testInvalidSchema() throws Exception {
        String schema = "--- random invalid schema ---" + new Date();

        SchemaMetadata schemaMetadataInfo = createSchemaInfo(displayName, SchemaCompatibility.BACKWARD);

        // registering a new schema
        Assertions.assertThrows(InvalidSchemaException.class, () -> schemaRegistry.addSchemaVersion(schemaMetadataInfo,
                new SchemaVersion(schema, "Initial version of the schema"))
                .getVersion());
    }

    @Test
    public void testSchemaTextAsNull() throws Exception {

        SchemaMetadata schemaMetadataInfo = createSchemaInfo(displayName, SchemaCompatibility.BACKWARD);

        // registering a new schema
        Assertions.assertThrows(InvalidSchemaException.class, () -> schemaRegistry.addSchemaVersion(schemaMetadataInfo,
                new SchemaVersion(null, "Initial version of the schema"))
                .getVersion());
    }

    @Test
    public void testSchemaTextAsEmpty() throws Exception {

        SchemaMetadata schemaMetadataInfo = createSchemaInfo(displayName, SchemaCompatibility.BACKWARD);

        // registering a new schema
        Assertions.assertThrows(InvalidSchemaException.class, () -> schemaRegistry.addSchemaVersion(schemaMetadataInfo,
                new SchemaVersion("", "Initial version of the schema"))
                .getVersion());
    }

    @Test
    public void testIncompatibleSchemas() throws Exception {
        String schema = getSchema("/device.avsc");
        String incompatSchema = getSchema("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaInfo(displayName, SchemaCompatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadata,
                                                     new SchemaVersion(schema, "Initial version of the schema"))
                                   .getVersion();

        // adding a new version of the schema
        Assertions.assertThrows(IncompatibleSchemaException.class, () -> schemaRegistry.addSchemaVersion(schemaMetadata,
                new SchemaVersion(incompatSchema, "second version"))
                .getVersion());
    }

    @Test
    public void testFindSchemaVersionByFingerprintSingle() throws Exception {
        final String schemaText = getSchema("/device.avsc");
        registerSchemaVersion(displayName, schemaText);

        final AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        final String fingerprint = Hex.encodeHexString(avroSchemaProvider.getFingerprint(schemaText));

        final SchemaVersionInfo schemaVersionFound = schemaRegistry.findSchemaVersionByFingerprint(fingerprint);

        Assertions.assertEquals(schemaText, schemaVersionFound.getSchemaText(), "Didn't find the expected schema version using the schema fingerprint");
    }

    @Test
    public void testFindSchemaVersionByFingerprintMultiple() throws Exception {
        final String schemaText = getSchema("/device.avsc");


        registerSchemaVersion("FirstSchema", schemaText);
        final SchemaIdVersion second = registerSchemaVersion("SecondSchema", schemaText);

        final AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        final String fingerprint = Hex.encodeHexString(avroSchemaProvider.getFingerprint(schemaText));

        final SchemaVersionInfo schemaVersionFound = schemaRegistry.findSchemaVersionByFingerprint(fingerprint);

        Assertions.assertEquals(second.getSchemaVersionId(), schemaVersionFound.getId(),
                "Didn't find the latest schema version using the schema fingerprint");
    }

    private SchemaIdVersion registerSchemaVersion(final String schemaName,
                                         final String schemaText) throws Exception {
        final SchemaMetadata schemaMetadata = createSchemaInfo(schemaName,
                SchemaCompatibility.DEFAULT_COMPATIBILITY);

        return schemaRegistry.addSchemaVersion(schemaMetadata,
                new SchemaVersion(schemaText, "Initial version of the schema"));
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
