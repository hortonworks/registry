/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility;

import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.SchemaRegistryHelper;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


/**
 * Encapsulation to ensure test case name is not duplicated resulting in loss of test cases.
 */
public class CompatibilityTestCase {
    @Getter
    private final String testName;
    private final ISchemaRegistryClient schemaRegistryClient;
    private final SchemaRegistryHelper schemaRegistryHelper;
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
     *
     * @param testName Name of the test
     */
    public CompatibilityTestCase(String testName, ISchemaRegistryClient schemaRegistryClient) {
        this.testName = testName;
        this.schemaRegistryClient = schemaRegistryClient;
        this.schemaRegistryHelper = new SchemaRegistryHelper();
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
            throw new RuntimeException("Exception thrown :" + ExceptionUtils.getStackTrace(exception));
        };
    }

    /**
     * Constructor that deep copies another test case but with a different test name.
     *
     * @param testName              name of the test
     * @param compatibilityTestCase test case to copy
     * @throws RuntimeException when provided invalid test action
     */
    public CompatibilityTestCase(String testName, CompatibilityTestCase compatibilityTestCase,
                                 ISchemaRegistryClient schemaRegistryClient) throws RuntimeException {
        this(testName, schemaRegistryClient);
        List<TestStep> steps = compatibilityTestCase.getTestSteps();
        for (TestStep step : steps) {
            TestStep newStep;
            switch (step.getTestAction()) {
                case ADD_SCHEMA_METADATA:
                    newStep = TestStep.builder()
                            .functionToBeExecuted(addSchemaMetadata)
                            .testObject(step.getTestObject())
                            .testAction(TestAction.ADD_SCHEMA_METADATA)
                            .build();
                    break;
                case ADD_VERSION:
                    newStep = TestStep.builder()
                            .functionToBeExecuted(addVersion)
                            .testObject(step.getTestObject())
                            .testAction(TestAction.ADD_VERSION)
                            .build();
                    break;
                case UPDATE_SCHEMA_METADATA:
                    newStep = TestStep.builder()
                            .functionToBeExecuted(updateSchemaMetadata)
                            .testObject(step.getTestObject())
                            .testAction(TestAction.UPDATE_SCHEMA_METADATA)
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
     *
     * @param propertiesInSchemaMetadata properties in schema metadata to change
     */
    public void addSchemaMetadata(Pair<SchemaRegistryHelper.SchemametadataFields, Object> propertiesInSchemaMetadata) {
        SchemaMetadata schemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(testName,
                "Registering " + testName, propertiesInSchemaMetadata);
        log.info("Adding step to add schema metadata {}", schemaMetadata);
        TestStep step = TestStep.builder()
                .functionToBeExecuted(addSchemaMetadata)
                .testObject(schemaMetadata)
                .testAction(TestAction.ADD_SCHEMA_METADATA)
                .build();
        testSteps.add(step);
    }

    /**
     * Add steps to add schema version.
     *
     * @param schema schema string to be added
     */
    public void addSchemaVersion(String schema) {
        SchemaVersion schemaVersion = new SchemaVersion(schema, "Adding a new version");
        log.info("Adding step to add schema version {} to schema {}", schema, testName);
        TestStep step = TestStep.builder()
                .functionToBeExecuted(addVersion)
                .testObject(schemaVersion)
                .testAction(TestAction.ADD_VERSION)
                .build();
        testSteps.add(step);
    }

    /**
     * Add steps to Update schema metadata.
     *
     * @param propertiesInSchemaMetadata properties in schema metadata to change
     */
    public void updateSchemaMetadata(Pair<SchemaRegistryHelper.SchemametadataFields, Object> propertiesInSchemaMetadata) {
        SchemaMetadata schemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(testName,
                "Registering " + testName, propertiesInSchemaMetadata);
        log.info("Adding step to update schema metadata of {} to {}", testName, schemaMetadata);
        TestStep step = TestStep.builder()
                .functionToBeExecuted(updateSchemaMetadata)
                .testObject(schemaMetadata)
                .testAction(TestAction.UPDATE_SCHEMA_METADATA)
                .build();
        testSteps.add(step);
    }

    /**
     * Add properties to test step to add exception.
     *
     * @param exceptionExpected Class of the expected exception
     * @param errorMessage      Error message in the expected exception
     * @return compatibility test case with the exception added
     */
    public CompatibilityTestCase expectExceptionAtTheLastStep(Class<RuntimeException> exceptionExpected,
                                                              String errorMessage) {
        return expectExceptionAtTheLastStep(exceptionExpected, errorMessage, null);
    }

    /**
     * Add properties to test step to add exception.
     *
     * @param exceptionExpected Class of the expected exception
     * @param errorMessage      Error message in the expected exception
     * @param bugID             Bug ID attached to the error
     * @return compatibility test case with the exception added
     */
    private CompatibilityTestCase expectExceptionAtTheLastStep(Class<RuntimeException> exceptionExpected,
                                                               String errorMessage,
                                                               String bugID) {
        TestStep lastTestStep = this.testSteps.get(this.testSteps.size() - 1);
        TestStep extendedTestStep = TestStep.builder()
                .functionToBeExecuted(lastTestStep.getFunctionToBeExecuted())
                .testObject(lastTestStep.getTestObject())
                .testAction(lastTestStep.getTestAction())
                .expectedException(exceptionExpected)
                .errorMessage(errorMessage)
                .bugID(bugID)
                .build();
        this.testSteps.remove(this.testSteps.size() - 1);
        this.testSteps.add(extendedTestStep);
        return this;
    }


}
