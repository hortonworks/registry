/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.utils.compatibility;

import lombok.Getter;
import java.util.HashMap;

/**
 * Encapsulation to ensure test case name is not duplicated resulting in loss of test cases.
 */
public class CompatibilityTestCases {
  @Getter
  private final HashMap<String, CompatibilityTestCase> testCases = new HashMap<>();

  /**
   * Append to test cases only if the name is unique. Throws runTimException if not unique.
   *
   * @param compatibilityTestCase Test case
   */
  public void append(CompatibilityTestCase compatibilityTestCase) {
    String testName = compatibilityTestCase.getTestName();
    if (testCases.get(testName) != null) {
      throw new RuntimeException("The testname " + testName + " seems to have appeared twice." +
          "\nList of existing tests :" + testCases.keySet().stream().map(s -> s + "\n"));
    }

    testCases.put(testName, compatibilityTestCase);
  }

  /**
   * Getter for test cases.
   *
   * @param testName Testname of the test case needed.
   * @return test cases.
   */
  public CompatibilityTestCase get(String testName) {
    return testCases.get(testName);
  }

  /**
   * Append testcases only if the name is unique. Throws runTimException if not unique.
   * @param compatibilityTestCases Maps of unique test cases
   */
  public void append(CompatibilityTestCases compatibilityTestCases) {
    compatibilityTestCases.getTestCases().forEach((testCaseName, testCase) -> append(testCase));
  }
}
