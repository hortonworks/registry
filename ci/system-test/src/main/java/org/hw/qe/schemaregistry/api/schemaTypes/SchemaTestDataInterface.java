/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.schemaTypes;

import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibiltyPositiveNegativeTestCases;

import java.util.ArrayList;

/**
 * Schema data interface for all data related methods.
 * @param <T> Schema type
 */
public interface SchemaTestDataInterface<T> {

  /**
   * Get the list of sample records to be tested.
   * @return List of sample records to be tested
   */
  ArrayList<T> getPossibleDatatypeSchemas();

  /**
   * List of schemas to be tested during compatibility.
   * @return schema which is compatible with older schema
   */
  CompatibiltyPositiveNegativeTestCases getPositiveCasesForCompatibility();

  /**
   * List of schemas to be tested during compatibility negative cases.
   * @return schema which is not compatible with older schema
   */
  CompatibiltyPositiveNegativeTestCases getNegativeCasesForCompatibility();
}
