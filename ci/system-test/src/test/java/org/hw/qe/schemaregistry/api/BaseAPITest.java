/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import org.hw.qe.schemaregistry.api.utils.SchemaRegistryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.slf4j.MDC;
import java.lang.reflect.Method;
import java.util.UUID;

import static org.hw.qe.schemaregistry.api.SchemaRegistryClientFactory.SchemaClientType.SCHEMA_REGISTRY_CLIENT;

/**
 * Tests for creating schema registry.
 */
public class BaseAPITest {
  private final ISchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.getSchemaClient(
      SCHEMA_REGISTRY_CLIENT);
  private final SchemaRegistryHelper schemaRegistryHelper = new SchemaRegistryHelper(schemaRegistryClient);
  private String testName = this.getClass().getSimpleName();
  private Long testStartTime;
  private String schemaName;
  private Logger log;

  static {
    MDC.put("loggerFileName", "BaseApiTestInitializer");
  }

  /**
   * Setup to run before running every test.
   * @param method Method being called
   */
  @BeforeMethod(alwaysRun = true)
  public void setupTestVariables(Method method) {
    testName = method.getName();
    testStartTime = System.currentTimeMillis();
    schemaName = 
        String.format("%s-%s--ts:%s--uuid:%s", 
            this.getClass().getSimpleName(), testName, testStartTime, UUID.randomUUID().toString());
    log.info("Started running test :" + testName);
  }

  /**
   * Setup to set log file name for test.
   */
  @BeforeClass(alwaysRun = true)
  public void setupClass() {
    String testFileName = this.getClass().getSimpleName();
    MDC.put("loggerFileName", testFileName);
    log = LoggerFactory.getLogger(testFileName);
  }

  /**
   * Tear down for a test method.
   */
  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    log.info("Finished running " + testName);
  }

  /**
   * Gets schemaRegistryClient
   *
   * @return value of schemaRegistryClient
   */
  public ISchemaRegistryClient getSchemaRegistryClient() {
    return schemaRegistryClient;
  }

  /**
   * Gets schemaRegistryHelper
   *
   * @return value of schemaRegistryHelper
   */
  public SchemaRegistryHelper getSchemaRegistryHelper() {
    return schemaRegistryHelper;
  }

  /**
   * Gets testName
   *
   * @return value of testName
   */
  public String getTestName() {
    return testName;
  }

  /**
   * Gets testStartTime
   *
   * @return value of testStartTime
   */
  public Long getTestStartTime() {
    return testStartTime;
  }

  /**
   * Gets schemaName
   *
   * @return value of schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public Logger getLog() {
    return log;
  }
}