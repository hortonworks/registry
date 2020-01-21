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
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import org.hw.qe.schemaregistry.api.BaseAPITest;
import org.hw.qe.schemaregistry.api.CompatibilityTestsDataProvider;
import org.hw.qe.schemaregistry.api.utils.ExpectException;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibilityTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Avro compatibility test.
 */
@Test
public class AvroSchemaCompatibilityTest extends BaseAPITest {

  /**
   * Test that when a schema is registered as backward compatible, it works.
   * @throws Exception Could be of type SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException
   * or others
   * @param testName Name of the test
   */
  @Test(dataProvider = "compatibilityTestCases", dataProviderClass = CompatibilityTestsDataProvider.class)
  public void testCompatibility(String testName) throws Exception {
    CompatibilityTestCase compatibilityTestCase = CompatibilityTestsDataProvider.get(testName);
    AtomicReference<Long> schemaMetadataID = new AtomicReference<>(null);
    List<SchemaVersion> schemaVersions = new ArrayList<>();

    // Test schema versions in pairs
    for (CompatibilityTestCase.TestStep step : compatibilityTestCase.getTestSteps()) {
      getLog().info("Step details : {}", step);
      getLog().info("Step {} : {} {}", compatibilityTestCase.getTestSteps().indexOf(step), step.getTestAction(),
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
          getLog().info("Schema metadata ID is : {}", schemaMetadataID.get());
        }

        // If step is add version, save the versions in a list for verification
        if (step.getTestAction() == CompatibilityTestCase.TestAction.ADD_VERSION) {
          schemaVersions.add((SchemaVersion) step.getTestObject());
          int versionsSize = schemaVersions.size();
          if (versionsSize > 1) {
            Collection<SchemaVersionInfo> actualSchemaVersions = getSchemaRegistryClient().getAllVersions(testName);
            Assert.assertEquals(actualSchemaVersions.size(), schemaVersions.size(), "Number of versions of schema");

            // Assert old versions
            Assert.assertTrue(schemaMetadataID.get() != null, "Add schema metadata failed");
            SchemaIdVersion oldVersion = new SchemaIdVersion(schemaMetadataID.get(), versionsSize - 1);
            getLog().info("Asserting old version with schema metadata ID {} and version # {}",
                schemaMetadataID.get(), versionsSize - 1);
            SchemaVersionInfo oldSchemaVersionInfo = getSchemaRegistryClient().getSchemaVersionInfo(oldVersion);
            getSchemaRegistryHelper().assertSchemaVersionInfo(testName,
                oldVersion,
                oldSchemaVersionInfo, "Old schema version info");

            // Assert new version
            SchemaIdVersion newVersion = new SchemaIdVersion(schemaMetadataID.get(), versionsSize);
            getLog().info("Asserting new version with schema metadata ID {} and version # {}",
                schemaMetadataID.get(), versionsSize);
            SchemaVersionInfo newSchemaVersionInfo = getSchemaRegistryClient().getSchemaVersionInfo(newVersion);
            getSchemaRegistryHelper().assertSchemaVersionInfo(testName,
                new SchemaIdVersion(schemaMetadataID.get(), versionsSize),
                newSchemaVersionInfo, "New schema version info");
          }
        }
      }
      getLog().info("Done with step {}", compatibilityTestCase.getTestSteps().indexOf(step));
    }
  }
}
