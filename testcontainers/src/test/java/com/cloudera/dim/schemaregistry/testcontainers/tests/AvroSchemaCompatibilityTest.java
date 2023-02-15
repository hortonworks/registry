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
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.CompatibilityTestCase;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.CompatibilityTestCases;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.TestStep;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.SchemaTestDataInterface;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.RecordSchemaType;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AvroSchemaCompatibilityTest extends TestcontainersTestsBase {

    private static Logger LOG = LoggerFactory.getLogger(AvroSchemaCompatibilityTest.class);
    private final static SchemaTestDataInterface SCHEMA_DATA = new RecordSchemaType();
    private static CompatibilityTestCases compatibilityTestCases;

    /**
     * Method to get test properties for a test.
     *
     * @param testName Name of the test
     * @return Test properties with test steps
     */
    public static CompatibilityTestCase get(String testName) {
        return compatibilityTestCases.get(testName);
    }

    public Stream<Arguments> compatibilityTestCases() {
        compatibilityTestCases = SCHEMA_DATA.getPositiveCasesForCompatibility(schemaRegistryClient).getAll();
        compatibilityTestCases.append(SCHEMA_DATA.getNegativeCasesForCompatibility(schemaRegistryClient).getAll());
        ArrayList<String> testCases = new ArrayList<>();
        // For each of the test cases in the compatible test cases,
        // create a test case array.
        for (String testName : compatibilityTestCases.getTestCases().keySet()) {
            testCases.add(testName);
        }


        Stream<Arguments> ret = testCases.stream()
                .map(testName -> Arguments.of(testName));

        return ret;
    }

    /**
     * Test that when a schema is registered as backward compatible, it works.
     *
     * @param testName Name of the test
     * @throws Exception Could be of type SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException
     *                   or others
     */
    @ParameterizedTest
    @MethodSource("compatibilityTestCases")
    // @Disabled("disabled during development, running too slowly")
    public void testCompatibility(String testName) throws Exception {
        CompatibilityTestCase compatibilityTestCase = get(testName);
        AtomicReference<Long> schemaMetadataID = new AtomicReference<>(null);
        List<SchemaVersion> schemaVersions = new ArrayList<>();

        // Test schema versions in pairs
        for (TestStep step : compatibilityTestCase.getTestSteps()) {
            LOG.info("Step details : {}", step);
            LOG.info("Step {} : {} {}", compatibilityTestCase.getTestSteps().indexOf(step), step.getTestAction(),
                    step.getTestObject());

            // If the step is expected to throw an exception, check exception is thrown.
            if (step.getExpectedException() != null) {
                ((ExpectException) () -> step.getFunctionToBeExecuted().apply(step.getTestObject()))
                        .assertExceptionThrown(step.getExpectedException(),
                                step.getErrorMessage(),
                                testName,
                                step.getBugID());
            } else {
                // Execute test step
                final Object result = step.getFunctionToBeExecuted().apply(step.getTestObject());

                // If step is add schema metadata, save schemaMetadataID for verification
                if (step.getTestAction() == CompatibilityTestCase.TestAction.ADD_SCHEMA_METADATA) {
                    schemaMetadataID.set((Long) result);
                    LOG.info("Schema metadata ID is : {}", schemaMetadataID.get());
                }

                // If step is add version, save the versions in a list for verification
                if (step.getTestAction() == CompatibilityTestCase.TestAction.ADD_VERSION) {
                    schemaVersions.add((SchemaVersion) step.getTestObject());
                    int versionsSize = schemaVersions.size();
                    if (versionsSize > 1) {
                        Collection<SchemaVersionInfo> actualSchemaVersions = schemaRegistryClient.getAllVersions(testName);
                        Assertions.assertEquals(actualSchemaVersions.size(), schemaVersions.size(), "Number of versions of schema");

                        // Assert old versions
                        Assertions.assertTrue(schemaMetadataID.get() != null, "Add schema metadata failed");
                        SchemaIdVersion oldVersion = new SchemaIdVersion(schemaMetadataID.get(), versionsSize - 1);
                        LOG.info("Asserting old version with schema metadata ID {} and version # {}",
                                schemaMetadataID.get(), versionsSize - 1);
                        SchemaVersionInfo oldSchemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(oldVersion);
                        schemaRegistryHelper.assertSchemaVersionInfo(schemaRegistryClient,
                                testName,
                                oldVersion,
                                oldSchemaVersionInfo,
                                "Old schema version info");

                        // Assert new version
                        SchemaIdVersion newVersion = new SchemaIdVersion(schemaMetadataID.get(), versionsSize);
                        LOG.info("Asserting new version with schema metadata ID {} and version # {}",
                                schemaMetadataID.get(), versionsSize);
                        SchemaVersionInfo newSchemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(newVersion);
                        schemaRegistryHelper.assertSchemaVersionInfo(
                                schemaRegistryClient,
                                testName,
                                new SchemaIdVersion(schemaMetadataID.get(), versionsSize),
                                newSchemaVersionInfo, "New schema version info");
                    }
                }
            }
            LOG.info("Done with step {}", compatibilityTestCase.getTestSteps().indexOf(step));
        }
    }
}
