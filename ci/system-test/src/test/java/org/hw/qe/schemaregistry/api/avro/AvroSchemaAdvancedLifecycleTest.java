/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.hw.qe.schemaregistry.api.BaseAPITest;
import org.hw.qe.schemaregistry.api.schemaTypes.avro.RecordSchemaType;
import org.hw.qe.schemaregistry.api.utils.AvroSchemaHelper;
import org.hw.qe.schemaregistry.api.utils.ExpectException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;

/**
 * Tests for creating schema registry.
 */
@Test
public class AvroSchemaAdvancedLifecycleTest extends BaseAPITest {

  private static final RecordSchemaType SCHEMA_DATA = new RecordSchemaType();
  private static final RecordSchemaType SCHEMA_TYPE = new RecordSchemaType();
  private static final Long DUMMY_VERSION_ID = 0L;
  private static Schema schema = SCHEMA_DATA.getPossibleDatatypeSchemas().get(0);
  private static String schemaString = SCHEMA_TYPE.getJson(schema);

  /**
   * Test to get a schema whose name doesn't exist.
   */
  @Test(enabled = false)
  public void testGettingNonExistentSchemaName() {
    SchemaMetadataInfo metadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo("NonExistentSchemaName");
    Assert.assertNotEquals(metadataInfo, null, "Getting schema metadata of non-existent schema." +
        "Check if BUG-87354 is fixed");
  }

  /**
   * Test to add a version to a schema whose name doesn't exist.
   */
  @Test
  public void testAddingVersionForNonExistentSchemaName() {
    ((ExpectException) () ->
        getSchemaRegistryClient().addSchemaVersion("NonExistentSchemaName", new SchemaVersion(schemaString,
            "Adding version to non-existent schema")))
        .assertExceptionThrown(SchemaNotFoundException.class,
            "Schema with name NonExistentSchemaName not found",
            "Updating non-existent schema");
  }

  /**
   * Test to check that updating an existing schema with invalid schema fails.
   * @throws SchemaNotFoundException When schema not found
   * or others
   */
  @Test(enabled = false)
  public void testRegisterInvalidSchema() throws SchemaNotFoundException {
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Registering schema");
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    String addString = AvroSchemaHelper.getInvalidSchemaString();
    getLog().info("Adding invalid version {} to schema {}", addString, getSchemaName());

    ((ExpectException) () ->
        getSchemaRegistryClient().addSchemaVersion(getSchemaName(), new SchemaVersion(addString, "Updating invalid schema")))
        .assertExceptionThrown(InvalidSchemaException.class, "", "Updating with invalid schema", "BUG-88059");

    // Get schema from the registry
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());
    SchemaMetadata registryMetadataFromGetRequest = schemaMetadataInfo.getSchemaMetadata();
    Assert.assertNotEquals(registryMetadataFromGetRequest, null,
        "The schema metadata of " + getSchemaName() + " was deleted while trying to update schema");
    Assert.assertEquals(getSchemaRegistryClient().getAllVersions(getSchemaName()).size(), 0,
        "Number of versions registered has increased even with invalid schema text");
  }


  /**
   * Test to register duplicate schema to schema registry.
   * @throws Exception Could be of type SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException
   * or others
   */
  @Test
  public void testRegisterDuplicateSchema() throws Exception {
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Registering schema");
    Long originalSchemaID = getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    SchemaVersion schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");
    SchemaIdVersion originalIdVersion = getSchemaRegistryClient().addSchemaVersion(getSchemaName(), schemaVersion);

    SchemaIdVersion idVersion = getSchemaRegistryClient().addSchemaVersion(getSchemaName(),
        new SchemaVersion(schemaString, "Registering duplicate schema"));
    Assert.assertEquals(idVersion, originalIdVersion, "ID version changed on duplicating schema");

    getSchemaRegistryHelper().assertSchemaMetadataInfo(getSchemaName(), originalSchemaMetadata, originalSchemaID,
        "Duplicating schema");

    Assert.assertEquals(getSchemaRegistryClient().getAllVersions(getSchemaName()).size(), 1,
        "Number of versions registered for old schema");
  }

  /**
   * Test to delete using schema ID that doesn't exist.
   */
  @Test
  public void testDeleteNonExistentSchemaID() {
    SchemaVersionKey schemaVersionKey = new SchemaVersionKey("NonExistentSchema", 1);
    ((ExpectException) () -> getSchemaRegistryClient().deleteSchemaVersion(schemaVersionKey)).
        assertExceptionThrown(SchemaNotFoundException.class,
            "Entity with id [NonExistentSchema] not found.",
            "Deleting non-existent schema");
  }

  /**
   * Test to get latest schema version that doesn't exist.
   */
  @Test(enabled = false)
  public void testGetNonExistentLatestSchemaVersion() {
    String expectedErrorMessage = String.format(
        "Entity with id [%s] not found.", getSchemaName());
    String exceptionThrownWhen = String.format("Getting non-existent latest version for schema %s", getSchemaName());
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Registering schema");
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);
    ((ExpectException) () ->
        getSchemaRegistryClient().getLatestSchemaVersionInfo(getSchemaName())).
        assertExceptionThrown(SchemaNotFoundException.class, expectedErrorMessage, exceptionThrownWhen,
            "BUG-87071");
  }

  /**
   * Test to get schema version that doesn't exist.
   */
  @Test(enabled = false)
  public void testGetNonExistentSchemaVersionInfo() {
    String expectedErrorMessage = String.format(
        "Entity with id [%s] not found.", getSchemaName());
    String exceptionThrownWhen = String.format("Getting non-existent version for schema %s", getSchemaName());
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Registering schema");
    Long schemaID = getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);
    ((ExpectException) () ->
        getSchemaRegistryClient().getSchemaVersionInfo(new SchemaIdVersion(schemaID, 1))).
        assertExceptionThrown(SchemaNotFoundException.class, expectedErrorMessage, exceptionThrownWhen,
            "BUG-87071");
  }

  /**
   * Test to add a valid version after adding an invalid version to a schema.
   * @throws Exception Could be of type SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException
   * or others
   */
  @Test(enabled = false)
  public void testAddValidVersionAfterInvalidVersion() throws Exception {
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Registering schema");
    getSchemaRegistryClient().registerSchemaMetadata(originalSchemaMetadata);

    // Adding version #1
    SchemaVersion schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");
    SchemaIdVersion originalSchemaIdVersion = getSchemaRegistryClient().addSchemaVersion(getSchemaName(), schemaVersion);
    Long originalSchemaID = originalSchemaIdVersion.getSchemaMetadataId();

    // Add new invalid version
    String invalidSchemaString = AvroSchemaHelper.getInvalidSchemaString();
    SchemaVersion updatedInvalidSchemaVersion = new SchemaVersion(invalidSchemaString, "Updating invalid schema");
    getSchemaRegistryClient().addSchemaVersion(getSchemaName(), updatedInvalidSchemaVersion);
    getLog().info("Adding new version {} to schema {}", invalidSchemaString, getSchemaName());

    // Assert schema metadataInfo is unchanged after update
    getSchemaRegistryHelper().assertSchemaMetadataInfo(getSchemaName(), originalSchemaMetadata, originalSchemaID,
        "Updating invalid schema");

    // Assert that the previous version is unchanged
    SchemaVersionInfo expectedSchemaVersionInfo = new SchemaVersionInfo(DUMMY_VERSION_ID, getSchemaName(), 1,
        schemaString, getTestStartTime(), "Adding version 1 to schema");
    getSchemaRegistryHelper().assertLatestSchemaVersionInfo(getSchemaName(), originalSchemaIdVersion,
        expectedSchemaVersionInfo, "Updating invalid schema");

    Long timeBeforeUpdate = System.currentTimeMillis();

    // Add new valid version
    Schema updatedSchema = SCHEMA_TYPE.updateSchemaJson(schema);
    String updatedSchemaString = SCHEMA_TYPE.getJson(updatedSchema);
    SchemaVersion updatedValidSchemaVersion = new SchemaVersion(updatedSchemaString, "Updating valid schema");
    SchemaIdVersion schemaIdVersion = getSchemaRegistryClient().addSchemaVersion(getSchemaName(), updatedValidSchemaVersion);
    getLog().info("Adding new version {} to schema {}", updatedSchemaString, getSchemaName());

    // Assert schema version is added after update
    SchemaVersionInfo expectedVersionInfoAfterValidUpdate = new SchemaVersionInfo(DUMMY_VERSION_ID, getSchemaName(), 2,
        updatedSchemaString, timeBeforeUpdate, "Updating valid schema");
    getSchemaRegistryHelper().assertLatestSchemaVersionInfo(getSchemaName(), schemaIdVersion, expectedVersionInfoAfterValidUpdate,
        "Updating valid schema");

    // Assert schema metadataInfo is unchanged after update
    getSchemaRegistryHelper().assertSchemaMetadataInfo(getSchemaName(), originalSchemaMetadata, originalSchemaID,
         "Updating valid schema");

    // Assert that the previous version is unchanged
    SchemaVersionKey schemaVersionKey = new SchemaVersionKey(getSchemaName(), 1);
    SchemaVersionInfo oldschemaVersionInfo = getSchemaRegistryClient().getSchemaVersionInfo(schemaVersionKey);
    SchemaVersionInfo originalSchemaVersionInfo = getSchemaRegistryClient().getSchemaVersionInfo(
        new SchemaVersionKey(getSchemaName(), 1));
    Assert.assertEquals(oldschemaVersionInfo, originalSchemaVersionInfo, "Old Updated string");
  }

  /**
   * Test to delete and add a schema version.
   * @throws Exception Could be of type SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException
   * or others
   */
  @Test
  public void testAddAfterDeleteSchemaVersion() throws Exception {
    // Register schema
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Registering schema");
    getSchemaRegistryClient().registerSchemaMetadata(originalSchemaMetadata);
    SchemaVersion schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");

    getSchemaRegistryClient().addSchemaVersion(getSchemaName(), schemaVersion);
    getSchemaRegistryClient().deleteSchemaVersion(new SchemaVersionKey(getSchemaName(), 1));

    await().atMost(10, TimeUnit.SECONDS).with().
        pollInterval(2, TimeUnit.SECONDS).
        until(() -> getSchemaRegistryClient().getAllVersions(getSchemaName()).size() == 0);

    if (getSchemaRegistryClient().getAllVersions(getSchemaName()).size() != 0) {
      Assert.fail("Schema " + getSchemaName() + " failed to delete");
    }

    schema = SCHEMA_DATA.getPossibleDatatypeSchemas().get(1);
    schemaString = SCHEMA_TYPE.getJson(schema);
    schemaVersion = new SchemaVersion(schemaString, "Adding version 1 to schema");

    SchemaIdVersion schemaIdVersion = getSchemaRegistryClient().addSchemaVersion(getSchemaName(), schemaVersion);
    getLog().info("Received from server: {}", schemaIdVersion);

    getSchemaRegistryHelper().assertLatestSchemaVersionInfo(getSchemaName(),
        schemaIdVersion,
        new SchemaVersionInfo(schemaIdVersion.getSchemaVersionId(), getSchemaName(),
            1, schemaString, getTestStartTime(), "Adding version 1 to schema"),
        "Updating after delete");
  }
}
