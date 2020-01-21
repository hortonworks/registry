/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.utils;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;


/**
 * Schema registry helper class.
 */
public final class SchemaRegistryHelper {
  private static final String DEFAULT_SCHEMA_GROUP = SchemaRegistryConstants.SchemaGroup.KAFKA.name().toLowerCase();
  public static final String DEFAULT_SCHEMA_TYPE = SchemaRegistryConstants.SchemaType.AVRO.name().toLowerCase();
  private final ISchemaRegistryClient schemaRegistryClient;

  /**
   * Fields related to a schema meta data that can be updated.
   */
  public enum SchemametadataFields {
    COMPATIBILITY,
    EVOLVE,
    TYPE,
    VALIDATIONLEVEL,
    SCHEMAGROUP
  }

  /**
   * Constructor for utility class.
   * @param schemaRegistryClient Schema registry client instance
   */
  public SchemaRegistryHelper(ISchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryClient = schemaRegistryClient;
  }

  /**
   * Get a schemametadata object with default values for all fields.
   * @param schemaName Schema name of the schema
   * @param description Description to determine schema
   * @param nonDefaultValueForField Non default key value pair
   * @return SchemaMetadata object to create schema
   */
  public SchemaMetadata getSchemaMetadataWithDefault(String schemaName, String description,
                                                     Pair<SchemametadataFields, Object> nonDefaultValueForField) {

    SchemaCompatibility compatibility = SchemaCompatibility.DEFAULT_COMPATIBILITY;
    boolean evolve = true;
    String schemaType = DEFAULT_SCHEMA_TYPE;
    SchemaValidationLevel validationLevel = SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL;
    String schemaGroup = DEFAULT_SCHEMA_GROUP;

    if (nonDefaultValueForField != null) {
      switch (nonDefaultValueForField.getKey()) {
        case TYPE:
          schemaType = (String) nonDefaultValueForField.getValue();
          break;
        case COMPATIBILITY:
          compatibility = (SchemaCompatibility) nonDefaultValueForField.getValue();
          break;
        case EVOLVE:
          evolve = (boolean) nonDefaultValueForField.getValue();
          break;
        case SCHEMAGROUP:
          schemaGroup = (String) nonDefaultValueForField.getValue();
          break;
        case VALIDATIONLEVEL:
          validationLevel = (SchemaValidationLevel) nonDefaultValueForField.getValue();
          break;
        default:
          throw new RuntimeException("The property " + nonDefaultValueForField.getKey() + " is not handled");
      }
    }
    return new SchemaMetadata.Builder(schemaName)
            .description(description)
            .compatibility(compatibility)
            .evolve(evolve)
            .type(schemaType)
            .validationLevel(validationLevel)
            .schemaGroup(schemaGroup)
            .build();
  }


  /**
   * Get a schemametadata object with default values for all fields.
   * @param schemaName Schema name of the schema
   * @param description Description to determine schema
   * @return SchemaMetadata object to create schema
   */
  public SchemaMetadata getSchemaMetadataWithDefault(String schemaName, String description) {
    return getSchemaMetadataWithDefault(schemaName, description, null);
  }

  /**
   * Method to assert schema metadata info.
   * @param schemaName Schema name to assert
   * @param expectedMetadata Expected metadata for the schema
   * @param expectedID Expected schema metadata ID
   * @param message Action during which assertion is done
   */
  public void assertSchemaMetadataInfo(String schemaName,
                                       SchemaMetadata expectedMetadata,
                                       Long expectedID,
                                       String message) {
    SchemaMetadataInfo actual = schemaRegistryClient.getSchemaMetadataInfo(schemaName);
    Assert.assertEquals(actual.getSchemaMetadata(), expectedMetadata, "Schema metadata comparison failed " +
            "while " + message);

    boolean expectedIDValid = actual.getId() > 0L;
    if (expectedID != null) {
      expectedIDValid = expectedIDValid && (expectedID.equals(actual.getId()));
    }
    Assert.assertTrue(expectedIDValid,
            "Id comparison " + actual.getId() + " and " + expectedID + " failed while " + message);
  }

  /**
   * Method to assert schema version info against an expected version number.
   * @param schemaName Schema name to assert
   * @param schemaIDVersion schemaIDVersion of the schema version
   * @param expectedSchemaVersionInfo Expected schema version info
   * @param message Action during which assertion is done
   * @throws SchemaNotFoundException throws exception when schema not found
   */
  public void assertSchemaVersionInfo(String schemaName,
                                      SchemaIdVersion schemaIDVersion,
                                      SchemaVersionInfo expectedSchemaVersionInfo,
                                      String message) throws SchemaNotFoundException {
    SchemaVersionInfo actualSchemaVersionInfoWithSchemaName;
    actualSchemaVersionInfoWithSchemaName = schemaRegistryClient.getSchemaVersionInfo(
            new SchemaVersionKey(schemaName, schemaIDVersion.getVersion()));
    runAssertionsForSchemaVersionInfo(actualSchemaVersionInfoWithSchemaName, expectedSchemaVersionInfo, message);

    Assert.assertEquals(actualSchemaVersionInfoWithSchemaName.getId(), expectedSchemaVersionInfo.getId(),
            "Schema version ID from get call different from schema ID version passed");

    Assert.assertEquals(actualSchemaVersionInfoWithSchemaName.getVersion(), schemaIDVersion.getVersion(),
            "Schema version number from get call different from schema ID version passed");

    SchemaVersionInfo actualVersionInfoWithSchemaID = schemaRegistryClient.getSchemaVersionInfo(schemaIDVersion);
    runAssertionsForSchemaVersionInfo(actualVersionInfoWithSchemaID, expectedSchemaVersionInfo, message);
  }

  /**
   * Method to assert latest schema version info.
   * @param schemaName Schema name to assert
   * @param schemaIDVersion schemaIDVersion of the schema version
   * @param expectedSchemaVersionInfo Expected schema version info
   * @param message Action during which assertion is done
   * @throws SchemaNotFoundException throws exception when schema not found
   */
  public void assertLatestSchemaVersionInfo(String schemaName,
                                            SchemaIdVersion schemaIDVersion,
                                            SchemaVersionInfo expectedSchemaVersionInfo,
                                            String message) throws SchemaNotFoundException {
    SchemaVersionInfo actualLatestSchemaVersionInfo;
    actualLatestSchemaVersionInfo = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
    runAssertionsForSchemaVersionInfo(actualLatestSchemaVersionInfo, expectedSchemaVersionInfo, message);
    Assert.assertEquals(schemaRegistryClient.getAllVersions(schemaName).size(),
            expectedSchemaVersionInfo.getVersion().intValue(),
            "There are more than the expected number of versions");
    assertSchemaVersionInfo(schemaName, schemaIDVersion, expectedSchemaVersionInfo, message);
  }

  /**
   * Method that runs assertions for schema version info.
   * @param actualSchemaVersionInfo Schema version info being asserted
   * @param expectedSchemaVersionInfo Expected schema version info
   * @param message Action during which assertion is done
   */
  private void runAssertionsForSchemaVersionInfo(SchemaVersionInfo actualSchemaVersionInfo,
                                                 SchemaVersionInfo expectedSchemaVersionInfo,
                                                 String message) {
    Assert.assertEquals(actualSchemaVersionInfo.getVersion(), expectedSchemaVersionInfo.getVersion(),
            "Schema version comparison failed while " + message);
    Assert.assertEquals(actualSchemaVersionInfo.getDescription(), expectedSchemaVersionInfo.getDescription(),
            "Description comparison failed while " + message);
    Assert.assertEquals(actualSchemaVersionInfo.getSchemaText(), expectedSchemaVersionInfo.getSchemaText(),
            "Schema text comparison failed while " + message);
    Assert.assertTrue(actualSchemaVersionInfo.getId() > 0, "Id comparison failed " +
            "while " + message);
    Assert.assertEquals(actualSchemaVersionInfo.getName(), expectedSchemaVersionInfo.getName(),
        "Name comparison failed while " + message);
    Assert.assertNotEquals(actualSchemaVersionInfo.getTimestamp(), 0);
  }

}
