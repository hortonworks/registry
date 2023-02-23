/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import org.apache.commons.lang3.tuple.Pair;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.CompatibilityTestCase;

import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.RANDOM_STRING;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_LONG_REQUIRED_FIELD;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_OPTIONAL_FIELD_MADE_REQUIRED;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_OPTIONAL_FIELD_ONLY;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_OPTIONAL_FIELD_RENAMED;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_REQUIRED_AND_OPTIONAL_FIELD;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_REQUIRED_FIELD_MADE_OPTIONAL;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_REQUIRED_FIELD_ONLY;
import static com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordConstants.SCHEMA_WITH_REQUIRED_FIELD_RENAMED;

import java.util.UUID;

/**
 * Test cases created for record type created to be reused.
 */
final class RecordCompatibilityTestCaseGenerator {

    private final boolean isPositiveTestCase;
    private final ISchemaRegistryClient schemaRegistryClient;

    /**
     * Private constructor for utility class.
     *
     * @param isPositiveTestCase is the test case a positive one
     */
    RecordCompatibilityTestCaseGenerator(boolean isPositiveTestCase,
                                         ISchemaRegistryClient schemaRegistryClient) {
        this.isPositiveTestCase = isPositiveTestCase;
        this.schemaRegistryClient = schemaRegistryClient;
    }

    /**
     * Private constructor for utility class.
     */
    RecordCompatibilityTestCaseGenerator(ISchemaRegistryClient schemaRegistryClient) {
        this(true, schemaRegistryClient);
    }

    /**
     * Change from optional field to required.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getOptionalFieldMadeRequired(Pair schemaMetaDataProperty) {
        CompatibilityTestCase optionalFieldMadeRequired =
                new CompatibilityTestCase(getTestName("OptionalFieldMadeRequired", schemaMetaDataProperty),
                        schemaRegistryClient);
        optionalFieldMadeRequired.addSchemaMetadata(schemaMetaDataProperty);
        optionalFieldMadeRequired.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_ONLY);
        optionalFieldMadeRequired.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_MADE_REQUIRED);
        return optionalFieldMadeRequired;
    }

    /**
     * Rename a field that has been declared as optional.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getRenameOptionalField(Pair schemaMetaDataProperty) {
        CompatibilityTestCase renameOptionalField =
                new CompatibilityTestCase(getTestName("RenameOptionalField", schemaMetaDataProperty),
                        schemaRegistryClient);
        renameOptionalField.addSchemaMetadata(schemaMetaDataProperty);
        renameOptionalField.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_ONLY);
        renameOptionalField.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_RENAMED);
        return renameOptionalField;
    }

    /**
     * Add a new required field.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getAddANewRequiredField(Pair schemaMetaDataProperty) {
        CompatibilityTestCase addANewRequiredField =
                new CompatibilityTestCase(getTestName("AddANewRequiredField", schemaMetaDataProperty),
                        schemaRegistryClient);
        addANewRequiredField.addSchemaMetadata(schemaMetaDataProperty);
        addANewRequiredField.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_ONLY);
        addANewRequiredField.addSchemaVersion(SCHEMA_WITH_REQUIRED_AND_OPTIONAL_FIELD);
        return addANewRequiredField;
    }

    /**
     * Add a new optional field.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getAddANewOptionalField(Pair schemaMetaDataProperty) {
        CompatibilityTestCase addANewOptionalField =
                new CompatibilityTestCase(getTestName("AddANewOptionalField", schemaMetaDataProperty),
                        schemaRegistryClient);
        addANewOptionalField.addSchemaMetadata(schemaMetaDataProperty);
        addANewOptionalField.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        addANewOptionalField.addSchemaVersion(SCHEMA_WITH_REQUIRED_AND_OPTIONAL_FIELD);
        return addANewOptionalField;
    }

    /**
     * Convert long to int.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getLongToInt(Pair schemaMetaDataProperty) {
        CompatibilityTestCase longToInt =
                new CompatibilityTestCase(getTestName("LongToInt", schemaMetaDataProperty),
                        schemaRegistryClient);
        longToInt.addSchemaMetadata(schemaMetaDataProperty);
        longToInt.addSchemaVersion(SCHEMA_WITH_LONG_REQUIRED_FIELD);
        longToInt.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        return longToInt;
    }

    /**
     * Field in old version was defined as required, but in the newer version we provide default value.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getRequiredFieldMadeOptional(Pair schemaMetaDataProperty) {
        CompatibilityTestCase requiredFieldMadeOptional =
                new CompatibilityTestCase(getTestName("RequiredFieldMadeOptional", schemaMetaDataProperty),
                        schemaRegistryClient);
        requiredFieldMadeOptional.addSchemaMetadata(schemaMetaDataProperty);
        requiredFieldMadeOptional.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        requiredFieldMadeOptional.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_MADE_OPTIONAL);
        return requiredFieldMadeOptional;
    }

    /**
     * Field in old version was defined as required, but is removed in newer version.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getRequiredFieldRemoved(Pair schemaMetaDataProperty) {
        CompatibilityTestCase requiredFieldRemoved =
                new CompatibilityTestCase(getTestName("RequiredFieldRemoved", schemaMetaDataProperty),
                        schemaRegistryClient);
        requiredFieldRemoved.addSchemaMetadata(schemaMetaDataProperty);
        requiredFieldRemoved.addSchemaVersion(SCHEMA_WITH_REQUIRED_AND_OPTIONAL_FIELD);
        requiredFieldRemoved.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_ONLY);
        return requiredFieldRemoved;
    }

    /**
     * Converting datatype int to long.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getIntToLong(Pair schemaMetaDataProperty) {
        CompatibilityTestCase intToLong =
                new CompatibilityTestCase(getTestName("IntToLong", schemaMetaDataProperty),
                        schemaRegistryClient);
        intToLong.addSchemaMetadata(schemaMetaDataProperty);
        intToLong.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        intToLong.addSchemaVersion(SCHEMA_WITH_LONG_REQUIRED_FIELD);
        return intToLong;
    }

    /**
     * Renaming a field and providing default value to it.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getRenamingAFieldAndAddingDefault(Pair schemaMetaDataProperty) {
        CompatibilityTestCase renamingAFieldAndAddingDefault =
                new CompatibilityTestCase(getTestName("RenamingAFieldAndAddingDefault", schemaMetaDataProperty),
                        schemaRegistryClient);
        renamingAFieldAndAddingDefault.addSchemaMetadata(schemaMetaDataProperty);
        renamingAFieldAndAddingDefault.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        renamingAFieldAndAddingDefault.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_RENAMED);
        return renamingAFieldAndAddingDefault;
    }

    /**
     * Creating a new field with default value should be backward compatible.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getNewFieldWithDefault(Pair schemaMetaDataProperty) {
        CompatibilityTestCase newFieldWithDefault =
                new CompatibilityTestCase(getTestName("NewFieldWithDefault", schemaMetaDataProperty),
                        schemaRegistryClient);
        newFieldWithDefault.addSchemaMetadata(schemaMetaDataProperty);
        newFieldWithDefault.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        newFieldWithDefault.addSchemaVersion(SCHEMA_WITH_REQUIRED_AND_OPTIONAL_FIELD);
        return newFieldWithDefault;
    }

    /**
     * Renaming a required field.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getRenamingARequiredField(Pair schemaMetaDataProperty) {
        CompatibilityTestCase renamingARequiredField =
                new CompatibilityTestCase(getTestName("RenamingARequiredField", schemaMetaDataProperty),
                        schemaRegistryClient);
        renamingARequiredField.addSchemaMetadata(schemaMetaDataProperty);
        renamingARequiredField.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        renamingARequiredField.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_RENAMED);
        return renamingARequiredField;
    }

    /**
     * AddingANewField.
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getAddingANewField(Pair schemaMetaDataProperty) {
        CompatibilityTestCase addingANewField =
                new CompatibilityTestCase(getTestName("AddingANewField", schemaMetaDataProperty),
                        schemaRegistryClient);
        addingANewField.addSchemaMetadata(schemaMetaDataProperty);
        addingANewField.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_ONLY);
        addingANewField.addSchemaVersion(SCHEMA_WITH_REQUIRED_AND_OPTIONAL_FIELD);
        return addingANewField;
    }

    /**
     * Create to a new schema not compatible with the previous (in any compatibility level other than None).
     *
     * @param schemaMetaDataProperty schema metadata properties to be changed
     * @return compatibility testcase
     */
    CompatibilityTestCase getRandomChange(Pair schemaMetaDataProperty) {
        CompatibilityTestCase randomChange =
                new CompatibilityTestCase(getTestName("RandomChange", schemaMetaDataProperty),
                        schemaRegistryClient);
        randomChange.addSchemaMetadata(schemaMetaDataProperty);
        randomChange.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_ONLY);
        randomChange.addSchemaVersion(RANDOM_STRING);
        return randomChange;
    }

    /**
     * Get test name with the schema metadata properties.
     *
     * @param testName               Test name to be created
     * @param schemaMetadataProperty schema metadata properties to be changed
     * @return test name string with the parameters
     */
    private String getTestName(String testName, Pair schemaMetadataProperty) {

        if (!isPositiveTestCase) {
            testName = testName + "_neg";
        }

        return testName
                + "_" + schemaMetadataProperty.getKey().toString().toLowerCase().replaceAll("[aeiou]", "")
                + "_" + schemaMetadataProperty.getValue().toString().toLowerCase()
                + "__st:" + System.currentTimeMillis()
                + "__UUID:" + UUID.randomUUID().toString();
    }
}
