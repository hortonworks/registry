/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests;

import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.RecordSchemaType;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AvroSchemaLifecycleTest extends TestcontainersTestsBase {

    private static Logger LOG = LoggerFactory.getLogger(AvroSchemaLifecycleTest.class);
    private static final RecordSchemaType SCHEMA_TYPE = new RecordSchemaType();
    private static final Schema SCHEMA = SCHEMA_TYPE.getPossibleDatatypeSchemas().get(0);
    private static final String SCHEMA_STRING = SCHEMA_TYPE.getJson(SCHEMA);

    private static final boolean DISABLE_CANONICAL_CHECK = true;
    private static final int EXPECTED_VERSION_ID_1 = 1;
    private static final int EXPECTED_VERSION_ID_2 = 2;


    private static Stream<Arguments> canonicalSchemaTestCases() {
        return Stream.of(
                Arguments.of(!DISABLE_CANONICAL_CHECK, EXPECTED_VERSION_ID_1),
                Arguments.of(DISABLE_CANONICAL_CHECK, EXPECTED_VERSION_ID_2)
        );
    }


    @Test
    public void testRegisterSchema() {
        SchemaMetadata schemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering SCHEMA");
        Long schemaID = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        schemaRegistryHelper.assertSchemaMetadataInfo(
                schemaRegistryClient,
                getSchemaName(), schemaMetadata, schemaID,
                "Registering SCHEMA");
    }


    @Test
    public void testAddVersion() throws Exception {
        SchemaMetadata schemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering SCHEMA");
        String description = "Adding version 1 to SCHEMA";
        schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        SchemaVersion schemaVersion = new SchemaVersion(SCHEMA_STRING, description);
        SchemaIdVersion idVersion = schemaRegistryClient.addSchemaVersion(getSchemaName(), schemaVersion);

        SchemaVersionInfo expectedSchemaVersionInfo = new SchemaVersionInfo(idVersion.getSchemaVersionId(),
                getSchemaName(), 1, SCHEMA_STRING, getTestStartTime(), description);
        schemaRegistryHelper.assertLatestSchemaVersionInfo(
                schemaRegistryClient,
                getSchemaName(), idVersion, expectedSchemaVersionInfo, description);

    }


    @Test
    public void testUpdateSchema() throws Exception {
        // Register SCHEMA
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering SCHEMA");
        schemaRegistryClient.registerSchemaMetadata(originalSchemaMetadata);

        // Add a version to SCHEMA
        SchemaVersion schemaVersion = new SchemaVersion(SCHEMA_STRING, "Adding version 1 to SCHEMA");
        Long originalSchemaID = schemaRegistryClient.addSchemaVersion(getSchemaName(), schemaVersion).getSchemaMetadataId();
        SchemaVersionInfo originalSchemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(
                new SchemaVersionKey(getSchemaName(), 1));

        // Add a second version
        Long timeBeforeUpdate = System.currentTimeMillis();
        Schema updatedSchema = SCHEMA_TYPE.updateSchemaJson(SCHEMA);
        String updatedSchemaString = SCHEMA_TYPE.getJson(updatedSchema);
        SchemaVersion updatedSchemaVersion = new SchemaVersion(updatedSchemaString, "Updating SCHEMA");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(getSchemaName(), updatedSchemaVersion);
        LOG.info("Adding new version {} to SCHEMA {}", updatedSchemaString, getSchemaName());

        // Assert SCHEMA version is added after update
        SchemaVersionInfo expectedSchemaVersionInfo = new SchemaVersionInfo(schemaIdVersion.getSchemaVersionId(),
                getSchemaName(), 2, updatedSchemaString, timeBeforeUpdate, "Updating SCHEMA");
        schemaRegistryHelper.assertLatestSchemaVersionInfo(
                schemaRegistryClient,
                getSchemaName(), schemaIdVersion, expectedSchemaVersionInfo,
                "Updating SCHEMA");

        // Assert SCHEMA metadataInfo is unchanged after update
        schemaRegistryHelper.assertSchemaMetadataInfo(
                schemaRegistryClient,
                getSchemaName(), originalSchemaMetadata, originalSchemaID,
                "Updating SCHEMA");

        // Assert that the previous version is unchanged
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(getSchemaName(), 1);
        SchemaVersionInfo oldschemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(schemaVersionKey);
        Assertions.assertEquals(oldschemaVersionInfo, originalSchemaVersionInfo,
                "Old Updated string");
    }


    @ParameterizedTest
    @MethodSource("canonicalSchemaTestCases")
    public void testUpdateWithCanonicallySameSchema(Boolean disableCanonicalCheck, Integer expectedVersionID) throws Exception {
        // create schema metadata
        SchemaMetadata schemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering SCHEMA");

        // Creating the JSON and adding the original version of the schema
        String originalSchemaJson = "{\"name\":\"record_name\",\"type\":\"record\",\"namespace\":\"hwqe.registries.avro.complex\",\"fields\":[]}";
        SchemaVersion originalSchemaVersion = new SchemaVersion(originalSchemaJson, "Adding first version of schema");
        schemaRegistryClient.addSchemaVersion(schemaMetadata, originalSchemaVersion);

        // Reorder type and name fields to create a JSON and a schema version which is canonically same like the original version
        String canonicallySameSchema = "{\"type\":\"record\",\"name\":\"record_name\",\"namespace\":\"hwqe.registries.avro.complex\",\"fields\":[]}";
        SchemaVersion canonicallySameSchemaVersion = new SchemaVersion(canonicallySameSchema,
                "Trying to add canonically similar version to SCHEMA");

        // Adding the canonically same version to the SCHEMA
        SchemaIdVersion updatedSchemaVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata,
                canonicallySameSchemaVersion, disableCanonicalCheck);
        Integer updatedVersionID = updatedSchemaVersion.getVersion();

        // Expects version ID to be 1 if disableCanonicalCheck was false, or to be 2 if disableCanonicalCheck was true
        Assertions.assertEquals(updatedVersionID, expectedVersionID);
    }


    @Test
    public void testDeleteSchemaVersion() throws Exception {
        // Register SCHEMA
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering SCHEMA");
        schemaRegistryClient.registerSchemaMetadata(originalSchemaMetadata);
        SchemaVersion schemaVersion = new SchemaVersion(SCHEMA_STRING, "Adding version 1 to SCHEMA");
        schemaRegistryClient.addSchemaVersion(getSchemaName(), schemaVersion);

        schemaRegistryClient.deleteSchemaVersion(new SchemaVersionKey(getSchemaName(), 1));

        Assertions.assertEquals(schemaRegistryClient.getAllVersions(getSchemaName()).size(), 0,
                "Delete of SCHEMA " + getSchemaName() + " unsuccessful");
    }
}
