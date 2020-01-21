/*
* Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
*
* Except as expressly permitted in a written agreement between your
* company and Hortonworks, Inc, any use, reproduction, modification,
* redistribution, sharing, lending or other exploitation of all or
* any part of the contents of this file is strictly prohibited.
*/
package org.hw.qe.schemaregistry.api;

import org.hw.qe.schemaregistry.api.schemaTypes.SchemaTestDataInterface;
import org.hw.qe.schemaregistry.api.schemaTypes.avro.RecordSchemaType;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibilityTestCase;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibilityTestCases;
import org.testng.annotations.DataProvider;
import java.util.ArrayList;

/**
 * Data provider for compatibility tests.
 */
public class CompatibilityTestsDataProvider {
  // TODO: Use factory to get schema type for different schemaTypes
  private final static SchemaTestDataInterface SCHEMA_DATA = new RecordSchemaType();
  private static CompatibilityTestCases compatibilityTestCases;

  /**
   * Data provider for compatible test cases.
   * First parameter : testName
   * @return 2 dimensional array of test cases
   */
  @DataProvider
  Object[][] compatibilityTestCases() {
    compatibilityTestCases = SCHEMA_DATA.getPositiveCasesForCompatibility().getAll();
    compatibilityTestCases.append(SCHEMA_DATA.getNegativeCasesForCompatibility().getAll());
    ArrayList<Object[]> testCases = new ArrayList<>();
    // For each of the test cases in the compatible test cases,
    // create a test case array.
    for (String testName : compatibilityTestCases.getTestCases().keySet()) {
      testCases.add(new Object[]{testName});
    }

    return testCases.toArray(new Object[testCases.size()][]);
  }

  /**
   * Method to get test properties for a test.
   * @param testName Name of the test
   * @return Test properties with test steps
   */
  public static CompatibilityTestCase get(String testName) {
    return compatibilityTestCases.get(testName);
  }
}
