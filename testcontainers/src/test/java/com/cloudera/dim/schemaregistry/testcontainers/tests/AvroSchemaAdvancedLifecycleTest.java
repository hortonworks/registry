/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests;

import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.ExpectException;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.RecordSchemaType;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AvroSchemaAdvancedLifecycleTest extends TestcontainersTestsBase {

    private static Logger LOG = LoggerFactory.getLogger(AvroSchemaAdvancedLifecycleTest.class);

    private static final RecordSchemaType SCHEMA_DATA = new RecordSchemaType();
    private static final RecordSchemaType SCHEMA_TYPE = new RecordSchemaType();
    private static final Long DUMMY_VERSION_ID = 0L;
    private static Schema schema = SCHEMA_DATA.getPossibleDatatypeSchemas().get(0);
    private static String schemaString = SCHEMA_TYPE.getJson(schema);


    @Test
    @Disabled("This was skipped at ci tests. Check before re-enable.")
    public void testGettingNonExistentSchemaName() {
        SchemaMetadataInfo metadataInfo = schemaRegistryClient.getSchemaMetadataInfo("NonExistentSchemaName");
        Assertions.assertNotEquals(metadataInfo, null, "Getting schema metadata of non-existent schema." +
                "Check if BUG-87354 is fixed");
    }


    @Test
    public void testAddingVersionForNonExistentSchemaName() {
        ((ExpectException) () ->
                schemaRegistryClient.addSchemaVersion("NonExistentSchemaName", new SchemaVersion(schemaString,
                        "Adding version to non-existent schema")))
                .assertExceptionThrown(SchemaNotFoundException.class,
                        "Schema with name NonExistentSchemaName not found",
                        "Updating non-existent schema");
    }


    @Test
    public void testRegisterInvalidSchema() throws SchemaNotFoundException {
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering schema");
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        String addString = getInvalidSchemaString();
        LOG.info("Adding invalid version {} to schema {}", addString, getSchemaName());

        ((ExpectException) () ->
                schemaRegistryClient.addSchemaVersion(getSchemaName(), new SchemaVersion(addString, "Updating invalid schema")))
                .assertExceptionThrown(InvalidSchemaException.class, "", "Updating with invalid schema", "BUG-88059");

        // Get schema from the registry
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());
        SchemaMetadata registryMetadataFromGetRequest = schemaMetadataInfo.getSchemaMetadata();
        Assertions.assertNotEquals(registryMetadataFromGetRequest, null,
                "The schema metadata of " + getSchemaName() + " was deleted while trying to update schema");
        Assertions.assertEquals(schemaRegistryClient.getAllVersions(getSchemaName()).size(), 0,
                "Number of versions registered has increased even with invalid schema text");
    }


    @Test
    public void testRegisterDuplicateSchema() throws Exception {
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering schema");
        Long originalSchemaID = schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        SchemaVersion schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");
        SchemaIdVersion originalIdVersion = schemaRegistryClient.addSchemaVersion(getSchemaName(), schemaVersion);

        SchemaIdVersion idVersion = schemaRegistryClient.addSchemaVersion(getSchemaName(),
                new SchemaVersion(schemaString, "Registering duplicate schema"));
        Assertions.assertEquals(idVersion, originalIdVersion, "ID version changed on duplicating schema");

        schemaRegistryHelper.assertSchemaMetadataInfo(
                schemaRegistryClient,
                getSchemaName(), originalSchemaMetadata, originalSchemaID,
                "Duplicating schema");

        Assertions.assertEquals(schemaRegistryClient.getAllVersions(getSchemaName()).size(), 1,
                "Number of versions registered for old schema");
    }


    @Test
    public void testDeleteNonExistentSchemaID() {
        String expectedEntitiy = "NonExistentSchema";
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(expectedEntitiy, 1);

        Exception exception = assertThrows(
                SchemaNotFoundException.class,
                () -> {
                    schemaRegistryClient.deleteSchemaVersion(schemaVersionKey);
                });

        String expectedMessage = "Entity with id [" + expectedEntitiy + "] not found.";

        assertTrue(exception instanceof SchemaNotFoundException);
        String actualMessage = exception.getMessage();
        String actualEntity = ((SchemaNotFoundException) exception).getEntity();
        assertTrue(actualMessage.contains(expectedMessage), "Deleting non-existent schema");
        assertTrue(actualEntity.contains(expectedEntitiy), "Deleting non-existent schema");
    }


    @Test
    @Disabled("This was skipped at ci tests. Check before re-enable.")
    public void testGetNonExistentLatestSchemaVersion() {
        String expectedErrorMessage = String.format(
                "Entity with id [%s] not found.", getSchemaName());
        String exceptionThrownWhen = String.format("Getting non-existent latest version for schema %s", getSchemaName());
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering schema");
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);
        ((ExpectException) () ->
                schemaRegistryClient.getLatestSchemaVersionInfo(getSchemaName())).
                assertExceptionThrown(SchemaNotFoundException.class, expectedErrorMessage, exceptionThrownWhen,
                        "BUG-87071");
    }


    @Test
    @Disabled("This was skipped at ci tests. Check before re-enable.")
    public void testGetNonExistentSchemaVersionInfo() {
        String expectedErrorMessage = String.format(
                "Entity with id [%s] not found.", getSchemaName());
        String exceptionThrownWhen = String.format("Getting non-existent version for schema %s", getSchemaName());
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering schema");
        Long schemaID = schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);
        ((ExpectException) () ->
                schemaRegistryClient.getSchemaVersionInfo(new SchemaIdVersion(schemaID, 1))).
                assertExceptionThrown(SchemaNotFoundException.class, expectedErrorMessage, exceptionThrownWhen,
                        "BUG-87071");
    }


    @Test
    @Disabled("This was skipped at ci tests. Check before re-enable.")
    public void testAddValidVersionAfterInvalidVersion() throws Exception {
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering schema");
        schemaRegistryClient.registerSchemaMetadata(originalSchemaMetadata);

        // Adding version #1
        SchemaVersion schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");
        SchemaIdVersion originalSchemaIdVersion = schemaRegistryClient.addSchemaVersion(getSchemaName(), schemaVersion);
        Long originalSchemaID = originalSchemaIdVersion.getSchemaMetadataId();

        // Add new invalid version
        String invalidSchemaString = getInvalidSchemaString();
        SchemaVersion updatedInvalidSchemaVersion = new SchemaVersion(invalidSchemaString, "Updating invalid schema");
        schemaRegistryClient.addSchemaVersion(getSchemaName(), updatedInvalidSchemaVersion);
        LOG.info("Adding new version {} to schema {}", invalidSchemaString, getSchemaName());

        // Assert schema metadataInfo is unchanged after update
        schemaRegistryHelper.assertSchemaMetadataInfo(
                schemaRegistryClient,
                getSchemaName(), originalSchemaMetadata, originalSchemaID,
                "Updating invalid schema");

        // Assert that the previous version is unchanged
        SchemaVersionInfo expectedSchemaVersionInfo = new SchemaVersionInfo(DUMMY_VERSION_ID, getSchemaName(), 1,
                schemaString, getTestStartTime(), "Adding version 1 to schema");
        schemaRegistryHelper.assertLatestSchemaVersionInfo(
                schemaRegistryClient,
                getSchemaName(), originalSchemaIdVersion,
                expectedSchemaVersionInfo, "Updating invalid schema");

        Long timeBeforeUpdate = System.currentTimeMillis();

        // Add new valid version
        Schema updatedSchema = SCHEMA_TYPE.updateSchemaJson(schema);
        String updatedSchemaString = SCHEMA_TYPE.getJson(updatedSchema);
        SchemaVersion updatedValidSchemaVersion = new SchemaVersion(updatedSchemaString, "Updating valid schema");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(getSchemaName(), updatedValidSchemaVersion);
        LOG.info("Adding new version {} to schema {}", updatedSchemaString, getSchemaName());

        // Assert schema version is added after update
        SchemaVersionInfo expectedVersionInfoAfterValidUpdate = new SchemaVersionInfo(DUMMY_VERSION_ID, getSchemaName(), 2,
                updatedSchemaString, timeBeforeUpdate, "Updating valid schema");
        schemaRegistryHelper.assertLatestSchemaVersionInfo(
                schemaRegistryClient,
                getSchemaName(), schemaIdVersion, expectedVersionInfoAfterValidUpdate,
                "Updating valid schema");

        // Assert schema metadataInfo is unchanged after update
        schemaRegistryHelper.assertSchemaMetadataInfo(
                schemaRegistryClient,
                getSchemaName(), originalSchemaMetadata, originalSchemaID,
                "Updating valid schema");

        // Assert that the previous version is unchanged
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(getSchemaName(), 1);
        SchemaVersionInfo oldschemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(schemaVersionKey);
        SchemaVersionInfo originalSchemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(
                new SchemaVersionKey(getSchemaName(), 1));
        Assertions.assertEquals(oldschemaVersionInfo, originalSchemaVersionInfo, "Old Updated string");
    }


    @Test
    public void testAddAfterDeleteSchemaVersion() throws Exception {
        String schemaName = getSchemaName();
        // Register schema
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(schemaName,
                "Registering schema");
        Long schemaMetadata = schemaRegistryClient.registerSchemaMetadata(originalSchemaMetadata);
        LOG.info("Metadata added: " + schemaMetadata);
        SchemaVersion schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");

        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaName, schemaVersion);
        LOG.info("Added version: " + schemaIdVersion1.getVersion() + " with globalVersionId: " +
                schemaIdVersion1.getSchemaVersionId());
        schemaRegistryClient.deleteSchemaVersion(new SchemaVersionKey(schemaName, 1));

        await().atMost(10, TimeUnit.SECONDS).with().
                pollInterval(2, TimeUnit.SECONDS).
                until(() -> schemaRegistryClient.getAllVersions(schemaName).size() == 0);

        if (schemaRegistryClient.getAllVersions(schemaName).size() != 0) {
            Assertions.fail("Schema " + schemaName + " failed to delete");
        }

        schema = SCHEMA_DATA.getPossibleDatatypeSchemas().get(1);
        schemaString = SCHEMA_TYPE.getJson(schema);
        schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");

        SchemaIdVersion schemaIdVersion2 = schemaRegistryClient.addSchemaVersion(schemaName, schemaVersion);
        LOG.info("Received from server: {}", schemaIdVersion2);

        schemaRegistryHelper.assertLatestSchemaVersionInfo(
                schemaRegistryClient,
                schemaName,
                schemaIdVersion2,
                new SchemaVersionInfo(schemaIdVersion2.getSchemaVersionId(), schemaName,
                        1, schemaString, getTestStartTime(), "Adding version 1 to schema"),
                "Updating after delete");
    }

    private String getInvalidSchemaString() {
        return "{\"abc\":\"xyz\"}";
    }
}
