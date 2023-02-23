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
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.CompatibilityTestCase;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.CompatibilityTestCases;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.CompatibiltyPositiveNegativeTestCases;

/**
 * Class with negative test cases for record type.
 */
public class RecordCompatibilityNegativeCases extends CompatibiltyPositiveNegativeTestCases {
    private final RecordCompatibilityTestCaseGenerator compatibilityTestGenerator = new
            RecordCompatibilityTestCaseGenerator(false, schemaRegistryClient);
    private final String incompatibilityErrorMessage =
            "Exception thrown :com.hortonworks.registries.schemaregistry.errors." +
                    "IncompatibleSchemaException: Given schema is not compatible with latest schema versions.";

    public RecordCompatibilityNegativeCases(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    /**
     * Negative test cases for compatibility.
     * Test case 1 : Renaming a required field will not be backward compatible.
     * Test case 2 : Add a field with no default will not be backward compatible.
     * Test case 3 : Add a new required field.
     *
     * @return Compatibility test cases
     */
    @Override
    protected CompatibilityTestCases getBackwardCompatibilityTests() {

        CompatibilityTestCases compatibilityTestCases = new CompatibilityTestCases();

        // Test case 1 : Renaming a field will not be backward compatible.
        compatibilityTestCases.append(
                compatibilityTestGenerator.getRenamingARequiredField(RecordConstants.SCHEMA_WITH_BACKWARD_COMPATIBILITY).
                        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

        // Test case 2 : Add a field with no default will not be backward compatible.
        compatibilityTestCases.append(compatibilityTestGenerator.getAddingANewField(RecordConstants.SCHEMA_WITH_BACKWARD_COMPATIBILITY).
                expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

        // Test case 3 : Add a new required field
        compatibilityTestCases.append(
                compatibilityTestGenerator.getAddANewRequiredField(RecordConstants.SCHEMA_WITH_BACKWARD_COMPATIBILITY).
                        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));
        return compatibilityTestCases;
    }

    /**
     * Test case 1 : Renaming a required field.
     * Test case 2 : Renaming a field and providing default value to it.
     * Test case 3 : Field in old version was defined as required, but is removed in newer version.
     *
     * @return Compatibility test cases
     */
    @Override
    protected CompatibilityTestCases getForwardCompatibilityTests() {
        CompatibilityTestCases compatibilityTestCases = new CompatibilityTestCases();

        // Test case 1 : Renaming a required field will not be forward compatible.
        compatibilityTestCases.append(
                compatibilityTestGenerator.getRenamingARequiredField(RecordConstants.SCHEMA_WITH_FORWARD_COMPATIBILITY).
                        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));


        // Test case 2 : Renaming a field and providing default value to it should be backward compatible.
        compatibilityTestCases.append(
                compatibilityTestGenerator.getRenamingAFieldAndAddingDefault(RecordConstants.SCHEMA_WITH_FORWARD_COMPATIBILITY).
                        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

        // Test case 3 : Field in old version was defined as required, but is removed in newer version.
        compatibilityTestCases.append(
                compatibilityTestGenerator.getRequiredFieldRemoved(RecordConstants.SCHEMA_WITH_FORWARD_COMPATIBILITY).
                        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));
        return compatibilityTestCases;
    }

    /**
     * None compatibility negative cases.
     *
     * @return Compatibility test cases.
     */
    @Override
    protected CompatibilityTestCases getNoneCompatibilityTests() {
        return new CompatibilityTestCases();
    }

    /**
     * Both compatibility negative cases.
     *
     * @return Compatibility test cases.
     */
    @Override
    protected CompatibilityTestCases getBothCompatibilityTests() {
        CompatibilityTestCases compatibilityTestCases = new CompatibilityTestCases();

        // Test case 1 : Add a new required field
        compatibilityTestCases.append(compatibilityTestGenerator.getAddANewRequiredField(RecordConstants.SCHEMA_WITH_BOTH_COMPATIBILITY).
                expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

        // Test case 2 : Renaming a field and providing default value to it should be backward compatible.
        compatibilityTestCases.append(
                compatibilityTestGenerator.getRenamingAFieldAndAddingDefault(RecordConstants.SCHEMA_WITH_BOTH_COMPATIBILITY).
                        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

        // Test case 3 : Field in old version was defined as required, but is removed in newer version.
        compatibilityTestCases.append(compatibilityTestGenerator.getRequiredFieldRemoved(RecordConstants.SCHEMA_WITH_BOTH_COMPATIBILITY).
                expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));
        return compatibilityTestCases;
    }

    /**
     * Test cases for update schema metadata with compatibility.
     * Test case 1 : testUpdateFromBackwardToForward
     * Test case 2 : testUpdateFromBothToBackward
     */
    @Override
    protected CompatibilityTestCases getUpdateTestCasesForCompatibility() {
        CompatibilityTestCases compatibilityTestCases = new CompatibilityTestCases();
        CompatibilityTestCase testUpdateFromBackwardToForward =
                new CompatibilityTestCase("UpdateFromBackwardToForward",
                        schemaRegistryClient);
        testUpdateFromBackwardToForward.addSchemaMetadata(RecordConstants.SCHEMA_WITH_BACKWARD_COMPATIBILITY);
        testUpdateFromBackwardToForward.addSchemaVersion(RecordConstants.SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        testUpdateFromBackwardToForward.updateSchemaMetadata(RecordConstants.SCHEMA_WITH_FORWARD_COMPATIBILITY);
        testUpdateFromBackwardToForward.addSchemaVersion(RecordConstants.SCHEMA_WITH_LONG_REQUIRED_FIELD);
        testUpdateFromBackwardToForward.expectExceptionAtTheLastStep(
                RuntimeException.class, incompatibilityErrorMessage);
        compatibilityTestCases.append(testUpdateFromBackwardToForward);

        CompatibilityTestCase testUpdateFromBothToBackward =
                new CompatibilityTestCase("UpdateFromBothToBackward",
                        schemaRegistryClient);
        testUpdateFromBothToBackward.addSchemaMetadata(RecordConstants.SCHEMA_WITH_BOTH_COMPATIBILITY);
        testUpdateFromBothToBackward.addSchemaVersion(RecordConstants.SCHEMA_WITH_LONG_REQUIRED_FIELD);
        testUpdateFromBothToBackward.updateSchemaMetadata(RecordConstants.SCHEMA_WITH_BACKWARD_COMPATIBILITY);
        testUpdateFromBothToBackward.addSchemaVersion(RecordConstants.SCHEMA_WITH_REQUIRED_FIELD_ONLY);
        testUpdateFromBothToBackward.expectExceptionAtTheLastStep(
                RuntimeException.class, incompatibilityErrorMessage);
        compatibilityTestCases.append(testUpdateFromBothToBackward);
        return compatibilityTestCases;
    }
}