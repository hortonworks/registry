/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.utils.compatibility;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hw.qe.schemaregistry.api.SchemaRegistryClientFactory;
import org.hw.qe.schemaregistry.api.utils.SchemaRegistryHelper;
import org.hw.qe.schemaregistry.api.utils.SchemaRegistryHelper.SchemametadataFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hw.qe.schemaregistry.api.SchemaRegistryClientFactory.SchemaClientType.SCHEMA_REGISTRY_CLIENT;

/**
 * Encapsulation to ensure test case name is not duplicated resulting in loss of test cases.
 */
public class CompatibilityTestCase {
  @Getter
  private final String testName;
  private final ISchemaRegistryClient schemaRegistryClient =
      SchemaRegistryClientFactory.getSchemaClient(SCHEMA_REGISTRY_CLIENT);
  private final SchemaRegistryHelper schemaRegistryHelper = new SchemaRegistryHelper(schemaRegistryClient);
  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private final Function addSchemaMetadata;
  private final Function updateSchemaMetadata;
  private final Function addVersion;
  @Getter
  private final List<TestStep> testSteps = new ArrayList<>();

  /**
   * Different kinds of test actions possible.
   */
  public enum TestAction {
    ADD_SCHEMA_METADATA,
    UPDATE_SCHEMA_METADATA,
    ADD_VERSION
  }

  /**
   * Constructor.
   * @param testName Name of the test
   */
  public CompatibilityTestCase(String testName) {
    this.testName = testName;
    addSchemaMetadata = schemaMetadata -> schemaRegistryClient.addSchemaMetadata((SchemaMetadata) schemaMetadata);
    updateSchemaMetadata = schemaMetadata ->
        schemaRegistryClient.updateSchemaMetadata(this.testName, (SchemaMetadata) schemaMetadata);
    addVersion = (schemaVersion) -> {
      Exception exception;
      try {
        return schemaRegistryClient.addSchemaVersion(this.testName, (SchemaVersion) schemaVersion);
      } catch (InvalidSchemaException | SchemaNotFoundException | IncompatibleSchemaException e) {
        exception = e;
      }
      throw new RuntimeException("Exception thrown :" + ExceptionUtils.getFullStackTrace(exception));
    };
  }

  /**
   * Constructor that deep copies another test case but with a different test name.
   * @param testName name of the test
   * @param compatibilityTestCase test case to copy
   * @throws RuntimeException when provided invalid test action
   */
  public CompatibilityTestCase(String testName, CompatibilityTestCase compatibilityTestCase) throws RuntimeException {
    this(testName);
    List<TestStep> steps = compatibilityTestCase.getTestSteps();
    for (TestStep step : steps) {
      TestStep newStep;
      switch (step.testAction) {
        case ADD_SCHEMA_METADATA:
          newStep = TestStep.builder(addSchemaMetadata,
              step.testObject,
              TestAction.ADD_SCHEMA_METADATA)
              .build();
          break;
        case ADD_VERSION:
          newStep = TestStep.builder(addVersion,
              step.testObject,
              TestAction.ADD_VERSION)
              .build();
          break;
        case UPDATE_SCHEMA_METADATA:
          newStep = TestStep.builder(updateSchemaMetadata,
              step.testObject,
              TestAction.UPDATE_SCHEMA_METADATA)
              .build();
          break;
        default:
          throw new RuntimeException("Test step of type " + step + " not handled");
      }
      testSteps.add(newStep);
    }
    log.info("Test steps :" + this.testSteps);
  }

  /**
   * Add steps to add schema metadata.
   * @param propertiesInSchemaMetadata properties in schema metadata to change
   */
  public void addSchemaMetadata(Pair<SchemametadataFields, Object> propertiesInSchemaMetadata) {
    SchemaMetadata schemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(testName,
        "Registering " + testName, propertiesInSchemaMetadata);
    log.info("Adding step to add schema metadata {}", schemaMetadata);
    TestStep step = TestStep.builder(addSchemaMetadata, schemaMetadata, TestAction.ADD_SCHEMA_METADATA).build();
    testSteps.add(step);
  }

  /**
   * Add steps to add schema version.
   * @param schema schema string to be added
   */
  public void addSchemaVersion(String schema) {
    SchemaVersion schemaVersion = new SchemaVersion(schema, "Adding a new version");
    log.info("Adding step to add schema version {} to schema {}", schema, testName);
    TestStep step = TestStep.builder(addVersion, schemaVersion, TestAction.ADD_VERSION).build();
    testSteps.add(step);
  }

  /**
   * Add steps to Update schema metadata.
   * @param propertiesInSchemaMetadata properties in schema metadata to change
   */
  public void updateSchemaMetadata(Pair<SchemametadataFields, Object> propertiesInSchemaMetadata) {
    SchemaMetadata schemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(testName,
        "Registering " + testName, propertiesInSchemaMetadata);
    log.info("Adding step to update schema metadata of {} to {}", testName, schemaMetadata);
    TestStep step = TestStep.builder(updateSchemaMetadata, schemaMetadata, TestAction.UPDATE_SCHEMA_METADATA)
        .build();
    testSteps.add(step);
  }

  /**
   * Add properties to test step to add exception.
   * @param exceptionExpected Class of the expected exception
   * @param errorMessage Error message in the expected exception
   * @return compatibility test case with the exception added
   */
  public CompatibilityTestCase expectExceptionAtTheLastStep(Class<RuntimeException> exceptionExpected,
                                                            String errorMessage) {
    return expectExceptionAtTheLastStep(exceptionExpected, errorMessage, null);
  }

  /**
   * Add properties to test step to add exception.
   * @param exceptionExpected Class of the expected exception
   * @param errorMessage Error message in the expected exception
   * @param bugID Bug ID attached to the error
   * @return compatibility test case with the exception added
   */
  private CompatibilityTestCase expectExceptionAtTheLastStep(Class<RuntimeException> exceptionExpected,
                                                            String errorMessage,
                                                            String bugID) {
    TestStep testStep = this.testSteps.get(this.testSteps.size() - 1);
    testStep.expectedException = exceptionExpected;
    testStep.errorMessage = errorMessage;
    testStep.bugID = bugID;
    return this;
  }

  /**
   * Class to represent a certain step.
   */
  @Getter
  @Builder(builderMethodName = "hiddenBuilder")
  @ToString
  public static final class TestStep {
    private final Function functionToBeExecuted;
    private final Object testObject;
    private final CompatibilityTestCase.TestAction testAction;
    private Class<? extends Exception> expectedException = null;
    private String errorMessage = null;
    private String bugID = null;

    /**
     * Builder method to add mandatory arguments.
     * @param functionToBeExecuted Function to be executed for the test step
     * @param testObject Test object for the test step
     * @param testAction Test action representing the function being executed
     * @return Builder object to build test step
     */
    static TestStepBuilder builder(Function functionToBeExecuted,
                                   Object testObject,
                                   TestAction testAction) {
      return hiddenBuilder().functionToBeExecuted(functionToBeExecuted).testObject(testObject).testAction(testAction);
    }

    /**
     * Clone method to deep copy.
     * @return A deep copy version of the test step
     */
    public TestStep clone() {
      return TestStep.builder(functionToBeExecuted, testObject, testAction).build();
    }
  }
}
