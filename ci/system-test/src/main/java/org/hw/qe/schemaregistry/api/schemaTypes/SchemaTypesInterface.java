/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.schemaTypes;

/**
 * Interface for all types of schema.
 * The generic type T is the object representation of the schema
 * For eg : for avro it is avro.Schema
 * @param <T> Type of schema
 */
public interface SchemaTypesInterface<T> {

  /**
   * Method to get the type of schema.
   * @return Type of schema
   */
  String getType();

  /**
   * Get json string for the schema.
   * @param schema Schema
   * @return json format of the schema object
   */
  String getJson(T schema);

  /**
   * Get a json that is valid update to an input json.
   * @param schemaToBeUpdated Schema to be updated
   * @return String to the updated json
   */
  T updateSchemaJson(T schemaToBeUpdated);
}
