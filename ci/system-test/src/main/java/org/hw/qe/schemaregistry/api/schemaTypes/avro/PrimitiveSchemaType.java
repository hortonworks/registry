/*
 * Copyright  (c) 2011-2020, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.schemaTypes.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.hw.qe.schemaregistry.api.schemaTypes.SchemaTestDataInterface;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibiltyPositiveNegativeTestCases;
import javax.ws.rs.NotSupportedException;
import java.util.ArrayList;

/**
 * Primitive avro data types.
 */
public class PrimitiveSchemaType implements AvroSchemaType, SchemaTestDataInterface<Schema> {
  /**
   * Get all types of primitive data type.
   * @return All types of primitive data type
   */
  static Schema.Type[] values() {
    return new Schema.Type[]{
        Schema.Type.NULL,
        Schema.Type.BYTES,
        Schema.Type.STRING,
        Schema.Type.BOOLEAN,
        Schema.Type.INT,
        Schema.Type.LONG,
        Schema.Type.FLOAT,
        Schema.Type.DOUBLE};
  }

  /**
   * Get primitive schema field's type.
   * @param type Type method of the schema
   * @return String form of the type
   */
  private String getType(Schema.Type type) {
    return type.name().toLowerCase();
  }

  /**
   * Get the list of sample records to be tested.
   * @return List of sample records to be tested
   */
  @Override
  public ArrayList<Schema> getPossibleDatatypeSchemas() {
    ArrayList<Schema> sampleSchemaJsons = new ArrayList<>();

    for (Schema.Type primitiveDataType : PrimitiveSchemaType.values()) {
      Schema primitiveType = SchemaBuilder
          .builder("abc")
          .type(getType(primitiveDataType));
      primitiveType.addProp("add", "ad");
      sampleSchemaJsons.add(primitiveType);
    }

    return sampleSchemaJsons;
  }

  @Override
  public CompatibiltyPositiveNegativeTestCases getPositiveCasesForCompatibility() {
    throw new NotSupportedException("getPositiveCasesForCompatibility not valid for primitive type");
  }

  @Override
  public CompatibiltyPositiveNegativeTestCases getNegativeCasesForCompatibility() {
    throw new NotSupportedException("getNegativeCasesForCompatibility not valid for primitive type");
  }

  @Override
  public Schema updateSchemaJson(Schema schemaToBeUpdated) {
    return schemaToBeUpdated;
  }

}
