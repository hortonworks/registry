/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibilityTestCase;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibilityTestCases;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibiltyPositiveNegativeTestCases;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_BACKWARD_COMPATIBILITY;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_BOTH_COMPATIBILITY;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_FORWARD_COMPATIBILITY;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_LONG_REQUIRED_FIELD;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_REQUIRED_FIELD_ONLY;

/**
 * Class with negative test cases for record type.
 */
public class RecordCompatibilityNegativeCases extends CompatibiltyPositiveNegativeTestCases {
  private final RecordCompatibilityTestCaseGenerator compatibilityTestGenerator = new
      RecordCompatibilityTestCaseGenerator(false);
  private final String incompatibilityErrorMessage =
      "Exception thrown :com.hortonworks.registries.schemaregistry.errors." +
          "IncompatibleSchemaException: Given schema is not compatible with latest schema versions.";

  /**
   * Negative test cases for compatibility.
   * Test case 1 : Renaming a required field will not be backward compatible.
   * Test case 2 : Add a field with no default will not be backward compatible.
   * Test case 3 : Add a new required field.
   * @return Compatibility test cases
   */
  @Override
  protected CompatibilityTestCases getBackwardCompatibilityTests() {

    CompatibilityTestCases compatibilityTestCases = new CompatibilityTestCases();

    // Test case 1 : Renaming a field will not be backward compatible.
    compatibilityTestCases.append(
        compatibilityTestGenerator.getRenamingARequiredField(SCHEMA_WITH_BACKWARD_COMPATIBILITY).
            expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

    // Test case 2 : Add a field with no default will not be backward compatible.
    compatibilityTestCases.append(compatibilityTestGenerator.getAddingANewField(SCHEMA_WITH_BACKWARD_COMPATIBILITY).
        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

    // Test case 3 : Add a new required field
    compatibilityTestCases.append(
        compatibilityTestGenerator.getAddANewRequiredField(SCHEMA_WITH_BACKWARD_COMPATIBILITY).
            expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));
    return compatibilityTestCases;
  }

  /**
   * Test case 1 : Renaming a required field.
   * Test case 2 : Renaming a field and providing default value to it.
   * Test case 3 : Field in old version was defined as required, but is removed in newer version.
   * @return Compatibility test cases
   */
  @Override
  protected CompatibilityTestCases getForwardCompatibilityTests() {
    CompatibilityTestCases compatibilityTestCases = new CompatibilityTestCases();

    // Test case 1 : Renaming a required field will not be forward compatible.
    compatibilityTestCases.append(
        compatibilityTestGenerator.getRenamingARequiredField(SCHEMA_WITH_FORWARD_COMPATIBILITY).
            expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));


    // Test case 2 : Renaming a field and providing default value to it should be backward compatible.
    compatibilityTestCases.append(
        compatibilityTestGenerator.getRenamingAFieldAndAddingDefault(SCHEMA_WITH_FORWARD_COMPATIBILITY).
            expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

    // Test case 3 : Field in old version was defined as required, but is removed in newer version.
    compatibilityTestCases.append(
        compatibilityTestGenerator.getRequiredFieldRemoved(SCHEMA_WITH_FORWARD_COMPATIBILITY).
            expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));
    return compatibilityTestCases;
  }

  /**
   * None compatibility negative cases.
   * @return Compatibility test cases.
   */
  @Override
  protected CompatibilityTestCases getNoneCompatibilityTests() {
    return new CompatibilityTestCases();
  }

  /**
   * Both compatibility negative cases.
   * @return Compatibility test cases.
   */
  @Override
  protected CompatibilityTestCases getBothCompatibilityTests() {
    CompatibilityTestCases compatibilityTestCases = new CompatibilityTestCases();

    // Test case 1 : Add a new required field
    compatibilityTestCases.append(compatibilityTestGenerator.getAddANewRequiredField(SCHEMA_WITH_BOTH_COMPATIBILITY).
        expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

    // Test case 2 : Renaming a field and providing default value to it should be backward compatible.
    compatibilityTestCases.append(
        compatibilityTestGenerator.getRenamingAFieldAndAddingDefault(SCHEMA_WITH_BOTH_COMPATIBILITY).
            expectExceptionAtTheLastStep(RuntimeException.class, incompatibilityErrorMessage));

    // Test case 3 : Field in old version was defined as required, but is removed in newer version.
    compatibilityTestCases.append(compatibilityTestGenerator.getRequiredFieldRemoved(SCHEMA_WITH_BOTH_COMPATIBILITY).
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
        new CompatibilityTestCase("UpdateFromBackwardToForward");
    testUpdateFromBackwardToForward.addSchemaMetadata(SCHEMA_WITH_BACKWARD_COMPATIBILITY);
    testUpdateFromBackwardToForward.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
    testUpdateFromBackwardToForward.updateSchemaMetadata(SCHEMA_WITH_FORWARD_COMPATIBILITY);
    testUpdateFromBackwardToForward.addSchemaVersion(SCHEMA_WITH_LONG_REQUIRED_FIELD);
    testUpdateFromBackwardToForward.expectExceptionAtTheLastStep(
        RuntimeException.class, incompatibilityErrorMessage);
    compatibilityTestCases.append(testUpdateFromBackwardToForward);

    CompatibilityTestCase testUpdateFromBothToBackward =
        new CompatibilityTestCase("UpdateFromBothToBackward");
    testUpdateFromBothToBackward.addSchemaMetadata(SCHEMA_WITH_BOTH_COMPATIBILITY);
    testUpdateFromBothToBackward.addSchemaVersion(SCHEMA_WITH_LONG_REQUIRED_FIELD);
    testUpdateFromBothToBackward.updateSchemaMetadata(SCHEMA_WITH_BACKWARD_COMPATIBILITY);
    testUpdateFromBothToBackward.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
    testUpdateFromBothToBackward.expectExceptionAtTheLastStep(
        RuntimeException.class, incompatibilityErrorMessage);
    compatibilityTestCases.append(testUpdateFromBothToBackward);
    return compatibilityTestCases;
  }
}