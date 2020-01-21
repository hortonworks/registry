/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.utils;

/**
 * Avro schema helper for all avro rest API calls.
 */
public final class AvroSchemaHelper {

  /**
   * Private constructor for utility class.
   */
  private AvroSchemaHelper() {

  }

  /**
   * Get an invalid string for negative test cases.
   * @return Object with request properties, expected values
   */
  public static String getInvalidSchemaString() {
    return "{\"abc\":\"xyz\"}";
  }

}
