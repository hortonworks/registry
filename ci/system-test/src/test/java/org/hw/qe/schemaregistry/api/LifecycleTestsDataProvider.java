/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api;

import org.testng.annotations.DataProvider;

/**
 * Data provider for lifecycle tests.
 */
public class LifecycleTestsDataProvider {
  private static final boolean DISABLE_CANONICAL_CHECK = true;
  private static final int EXPECTED_VERSION_ID_1 = 1;
  private static final int EXPECTED_VERSION_ID_2 = 2;

  /**
   * Data provider for lifecycle test cases.
   * First parameter : disableCanonicalCheck
   * Second Parameter : expectedVersionID
   *
   * @return 2 dimensional array of test cases
   */
  @DataProvider
  Object[][] canonicalSchemaTestCases() {
    return new Object[][]{{!DISABLE_CANONICAL_CHECK, EXPECTED_VERSION_ID_1}, {DISABLE_CANONICAL_CHECK, EXPECTED_VERSION_ID_2}};
  }
}
