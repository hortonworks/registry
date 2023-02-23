/*
 * Copyright  (c) 2011-2020, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class SchemaRegistryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryHelper.class);

    private static final String DEFAULT_SCHEMA_GROUP = "kafka";
    public static final String DEFAULT_SCHEMA_TYPE = AvroSchemaProvider.TYPE.toLowerCase();

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
     * Get a schemametadata object with default values for all fields.
     *
     * @param schemaName              Schema name of the schema
     * @param description             Description to determine schema
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
     *
     * @param schemaName  Schema name of the schema
     * @param description Description to determine schema
     * @return SchemaMetadata object to create schema
     */
    public SchemaMetadata getSchemaMetadataWithDefault(String schemaName, String description) {
        return getSchemaMetadataWithDefault(schemaName, description, null);
    }

    /**
     * Method to assert schema metadata info.
     *
     * @param schemaName       Schema name to assert
     * @param expectedMetadata Expected metadata for the schema
     * @param expectedID       Expected schema metadata ID
     * @param message          Action during which assertion is done
     */
    public void assertSchemaMetadataInfo(ISchemaRegistryClient schemaRegistryClient,
                                         String schemaName,
                                         SchemaMetadata expectedMetadata,
                                         Long expectedID,
                                         String message) {

        SchemaMetadataInfo actual = schemaRegistryClient.getSchemaMetadataInfo(schemaName);
        LOG.info("Schema metadata for \"{}\": {}", schemaName, actual);
        Assert.assertEquals("Schema metadata comparison failed " +
                        "while " + message,
                actual.getSchemaMetadata(), expectedMetadata);

        boolean expectedIDValid = actual.getId() > 0L;
        if (expectedID != null) {
            expectedIDValid = expectedIDValid && (expectedID.equals(actual.getId()));
        }
        Assert.assertTrue("Id comparison " + actual.getId() + " and " + expectedID + " failed while " + message,
                expectedIDValid);
    }

    /**
     * Method to assert schema version info against an expected version number.
     *
     * @param schemaName                Schema name to assert
     * @param schemaIDVersion           schemaIDVersion of the schema version
     * @param expectedSchemaVersionInfo Expected schema version info
     * @param message                   Action during which assertion is done
     * @throws SchemaNotFoundException throws exception when schema not found
     */
    public void assertSchemaVersionInfo(ISchemaRegistryClient schemaRegistryClient,
                                        String schemaName,
                                        SchemaIdVersion schemaIDVersion,
                                        SchemaVersionInfo expectedSchemaVersionInfo,
                                        String message) throws SchemaNotFoundException {

        SchemaVersionInfo actualSchemaVersionInfoWithSchemaName = schemaRegistryClient.getSchemaVersionInfo(
                new SchemaVersionKey(schemaName, schemaIDVersion.getVersion()));
        LOG.info("Actual schema version for \"{}\" and version {} is: {}", schemaName, schemaIDVersion, actualSchemaVersionInfoWithSchemaName);
        runAssertionsForSchemaVersionInfo(actualSchemaVersionInfoWithSchemaName, expectedSchemaVersionInfo, message);

        Assert.assertEquals("Schema version ID from get call different from schema ID version passed",
                actualSchemaVersionInfoWithSchemaName.getId(), expectedSchemaVersionInfo.getId());

        Assert.assertEquals("Schema version number from get call different from schema ID version passed",
                actualSchemaVersionInfoWithSchemaName.getVersion(), schemaIDVersion.getVersion());

        SchemaVersionInfo actualVersionInfoWithSchemaID = schemaRegistryClient.getSchemaVersionInfo(schemaIDVersion);
        LOG.info("Schema for version id {} is: {}", schemaIDVersion, actualVersionInfoWithSchemaID);
        runAssertionsForSchemaVersionInfo(actualVersionInfoWithSchemaID, expectedSchemaVersionInfo, message);
    }

    /**
     * Method to assert latest schema version info.
     *
     * @param schemaName                Schema name to assert
     * @param schemaIDVersion           schemaIDVersion of the schema version
     * @param expectedSchemaVersionInfo Expected schema version info
     * @param message                   Action during which assertion is done
     * @throws SchemaNotFoundException throws exception when schema not found
     */
    public void assertLatestSchemaVersionInfo(ISchemaRegistryClient schemaRegistryClient,
                                              String schemaName,
                                              SchemaIdVersion schemaIDVersion,
                                              SchemaVersionInfo expectedSchemaVersionInfo,
                                              String message) throws SchemaNotFoundException {
        SchemaVersionInfo actualLatestSchemaVersionInfo = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
        LOG.info("Latest schema versionInfo for \"{}\" is: {}", schemaName, actualLatestSchemaVersionInfo);
        LOG.info("Expected versionInfo is: {}", expectedSchemaVersionInfo);
        runAssertionsForSchemaVersionInfo(actualLatestSchemaVersionInfo, expectedSchemaVersionInfo, message);
        Assertions.assertEquals(schemaRegistryClient.getAllVersions(schemaName).size(),
                expectedSchemaVersionInfo.getVersion().intValue(),
                "There are more than the expected number of versions");
        assertSchemaVersionInfo(schemaRegistryClient, schemaName, schemaIDVersion, expectedSchemaVersionInfo, message);
    }

    /**
     * Method that runs assertions for schema version info.
     *
     * @param actualSchemaVersionInfo   Schema version info being asserted
     * @param expectedSchemaVersionInfo Expected schema version info
     * @param message                   Action during which assertion is done
     */
    private void runAssertionsForSchemaVersionInfo(SchemaVersionInfo actualSchemaVersionInfo,
                                                   SchemaVersionInfo expectedSchemaVersionInfo,
                                                   String message) {
        Assert.assertEquals("Schema version comparison failed while " + message,
                actualSchemaVersionInfo.getVersion(), expectedSchemaVersionInfo.getVersion());
        Assert.assertEquals("Description comparison failed while " + message,
                actualSchemaVersionInfo.getDescription(), expectedSchemaVersionInfo.getDescription());
        Assert.assertEquals("Schema text comparison failed while " + message,
                actualSchemaVersionInfo.getSchemaText(), expectedSchemaVersionInfo.getSchemaText());
        Assert.assertTrue("Id comparison failed " +
                        "while " + message,
                actualSchemaVersionInfo.getId() > 0);
        Assert.assertEquals("Name comparison failed while " + message,
                actualSchemaVersionInfo.getName(), expectedSchemaVersionInfo.getName());
        Assert.assertNotEquals((long) actualSchemaVersionInfo.getTimestamp(), 0);
    }

}
