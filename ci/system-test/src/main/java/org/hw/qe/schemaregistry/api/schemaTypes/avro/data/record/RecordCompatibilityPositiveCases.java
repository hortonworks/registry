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
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_NONE_COMPATIBILITY;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_OPTIONAL_FIELD_MADE_REQUIRED;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_OPTIONAL_FIELD_ONLY;
import static org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordConstants.SCHEMA_WITH_REQUIRED_FIELD_ONLY;

/**
 * Class with positive cases for record type.
 */
public class RecordCompatibilityPositiveCases extends CompatibiltyPositiveNegativeTestCases {
  private final RecordCompatibilityTestCaseGenerator compatibilityTestGenerator = new
      RecordCompatibilityTestCaseGenerator();
  /**
   * Test cases for forward compatibility.
   * Test case 1 : Change from optional field to required
   * Test case 2 : Rename a field that has been declared as optional
   * Test case 3 : Add a new required field
   * Test case 4 : Add a new optional field
   * Test case 5 : Convert long to int
   * Test case 6 : Field in old version was defined as required, but in the newer version we provide default value
   * Test case 7 : Creating a new field with default value
   */
  @Override
  public CompatibilityTestCases getForwardCompatibilityTests() {
    CompatibilityTestCases positiveCases = new CompatibilityTestCases();

    // Test case 1 : Change from optional field to required
    positiveCases.append(compatibilityTestGenerator.getOptionalFieldMadeRequired(SCHEMA_WITH_FORWARD_COMPATIBILITY));

    // Test case 2 : Rename a field that has been declared as optional
    positiveCases.append(compatibilityTestGenerator.getRenameOptionalField(SCHEMA_WITH_FORWARD_COMPATIBILITY));

    // Test case 3 : Add a new required field
    positiveCases.append(compatibilityTestGenerator.getAddANewRequiredField(SCHEMA_WITH_FORWARD_COMPATIBILITY));

    // Test case 4 : Add a new optional field
    positiveCases.append(compatibilityTestGenerator.getAddANewOptionalField(SCHEMA_WITH_FORWARD_COMPATIBILITY));

    // Test case 5 : Convert long to int
    positiveCases.append(compatibilityTestGenerator.getLongToInt(SCHEMA_WITH_FORWARD_COMPATIBILITY));

    // Test case 6 : Field in old version was defined as required, but in the newer version we provide default value
    positiveCases.append(compatibilityTestGenerator.getRequiredFieldMadeOptional(SCHEMA_WITH_FORWARD_COMPATIBILITY));

    // Test case 7 : Creating a new field with default value.
    positiveCases.append(compatibilityTestGenerator.getNewFieldWithDefault(SCHEMA_WITH_FORWARD_COMPATIBILITY));

    return positiveCases;
  }

  /**
   * Test cases for backward compatibility.
   * Test case 1 : Field in old version was defined as required, but in the newer version we provide default value.
   * Test case 2 : Field in old version was defined as required, but is removed in newer version.
   * Test case 3 : Converting datatype int to long.
   * Test case 4 : Renaming a field and providing default value to it.
   * Test case 5 : Creating a new field with default value.
   * Test case 6 : Change from optional field to required.
   * Test case 7 : Rename a field that has been declared as optional.
   * Test case 8 : Add a new optional field.
   */
  @Override
  public CompatibilityTestCases getBackwardCompatibilityTests() {
    CompatibilityTestCases positiveCases = new CompatibilityTestCases();

    // Test case 1 : Field in old version was defined as required, but in the newer version we provide default value
    positiveCases.append(compatibilityTestGenerator.getRequiredFieldMadeOptional(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    // Test case 2 : Field in old version was defined as required, but is removed in newer version.
    positiveCases.append(compatibilityTestGenerator.getRequiredFieldRemoved(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    // Test case 3 : Converting datatype int to long.
    positiveCases.append(compatibilityTestGenerator.getIntToLong(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    // Test case 4 : Renaming a field and providing default value to it.
    positiveCases.append(compatibilityTestGenerator.
        getRenamingAFieldAndAddingDefault(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    // Test case 5 : Creating a new field with default value.
    positiveCases.append(compatibilityTestGenerator.getNewFieldWithDefault(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    // Test case 6 : Change from optional field to required
    positiveCases.append(compatibilityTestGenerator.getOptionalFieldMadeRequired(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    // Test case 7 : Rename a field that has been declared as optional
    positiveCases.append(compatibilityTestGenerator.getRenameOptionalField(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    // Test case 8 : Add a new optional field
    positiveCases.append(compatibilityTestGenerator.getAddANewOptionalField(SCHEMA_WITH_BACKWARD_COMPATIBILITY));

    return positiveCases;
  }

  /**
   * Test cases for none compatibility.
   * Test case 1 : Change to a completely new schema
   */
  @Override
  public CompatibilityTestCases getNoneCompatibilityTests() {
    CompatibilityTestCases positiveCases = new CompatibilityTestCases();
    // Test case 1 : Change to a completely new schema
    positiveCases.append(compatibilityTestGenerator.getRandomChange(SCHEMA_WITH_NONE_COMPATIBILITY));
    return positiveCases;
  }

  /**
   * Test cases for both compatibility.
   * Test case 1 : Change from optional field to required.
   * Test case 2 : Rename a field that has been declared as optional
   * Test case 3 : Add a new optional field
   * Test case 4 : Field in old version was defined as required, but in the newer version we provide default value
   * Test case 5 : Creating a new field with default value.
   */
  @Override
  protected CompatibilityTestCases getBothCompatibilityTests() {
    CompatibilityTestCases positiveCases = new CompatibilityTestCases();

    // Test case 1 : Change from optional field to required
    positiveCases.append(compatibilityTestGenerator.getOptionalFieldMadeRequired(SCHEMA_WITH_BOTH_COMPATIBILITY));

    // Test case 2 : Rename a field that has been declared as optional
    positiveCases.append(compatibilityTestGenerator.getRenameOptionalField(SCHEMA_WITH_BOTH_COMPATIBILITY));

    // Test case 3 : Add a new optional field
    positiveCases.append(compatibilityTestGenerator.getAddANewOptionalField(SCHEMA_WITH_BOTH_COMPATIBILITY));

    // Test case 4 : Field in old version was defined as required, but in the newer version we provide default value
    positiveCases.append(compatibilityTestGenerator.getRequiredFieldMadeOptional(SCHEMA_WITH_BOTH_COMPATIBILITY));

    // Test case 5 : Creating a new field with default value.
    positiveCases.append(compatibilityTestGenerator.getNewFieldWithDefault(SCHEMA_WITH_BOTH_COMPATIBILITY));
    return positiveCases;
  }

  /**
   * Test cases for update schema metadata with compatibility.
   * Test case 1 : testUpdateFromBackwardToForward.
   * Test case 2 : testUpdateFromBothToBackward
   * Test case 3 : testUpdateFromNoneToBoth
   */
  @Override
  protected CompatibilityTestCases getUpdateTestCasesForCompatibility() {
    CompatibilityTestCases testCases = new CompatibilityTestCases();
    CompatibilityTestCase testUpdateFromBackwardToForward =
        new CompatibilityTestCase("testUpdateFromBackwardToForward");
    testUpdateFromBackwardToForward.addSchemaMetadata(SCHEMA_WITH_BACKWARD_COMPATIBILITY);
    testUpdateFromBackwardToForward.addSchemaVersion(SCHEMA_WITH_LONG_REQUIRED_FIELD);
    testUpdateFromBackwardToForward.updateSchemaMetadata(SCHEMA_WITH_FORWARD_COMPATIBILITY);
    testUpdateFromBackwardToForward.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);

    CompatibilityTestCase testUpdateFromBothToBackward =
        new CompatibilityTestCase("testUpdateFromBothToBackward");
    testUpdateFromBothToBackward.addSchemaMetadata(SCHEMA_WITH_BOTH_COMPATIBILITY);
    testUpdateFromBothToBackward.addSchemaVersion(SCHEMA_WITH_REQUIRED_FIELD_ONLY);
    testUpdateFromBothToBackward.updateSchemaMetadata(SCHEMA_WITH_BACKWARD_COMPATIBILITY);
    testUpdateFromBothToBackward.addSchemaVersion(SCHEMA_WITH_LONG_REQUIRED_FIELD);
    testCases.append(testUpdateFromBothToBackward);

    CompatibilityTestCase testUpdateFromNoneToBoth =
        new CompatibilityTestCase("testUpdateFromNoneToBoth");
    testUpdateFromNoneToBoth.addSchemaMetadata(SCHEMA_WITH_NONE_COMPATIBILITY);
    testUpdateFromNoneToBoth.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_ONLY);
    testUpdateFromNoneToBoth.updateSchemaMetadata(SCHEMA_WITH_BOTH_COMPATIBILITY);
    testUpdateFromNoneToBoth.addSchemaVersion(SCHEMA_WITH_OPTIONAL_FIELD_MADE_REQUIRED);
    testCases.append(testUpdateFromNoneToBoth);
    return testCases;
  }

}