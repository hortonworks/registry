/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.utils;

import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Constants needed for schema registry.
 */
public class SchemaRegistryConstants {

  /**
   * Enum for types of schema groups.
   */
  public enum SchemaGroup {
    KAFKA
  }

  /**
   * Enum for types of schema types.
   */
  @Getter
  @AllArgsConstructor
  public enum SchemaType {
    AVRO(AvroSchemaProvider.TYPE);
    private String type;
  }

}
