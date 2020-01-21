/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.schemaTypes.avro;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.ToString;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.hw.qe.schemaregistry.api.schemaTypes.SchemaTestDataInterface;
import org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordCompatibilityNegativeCases;
import org.hw.qe.schemaregistry.api.schemaTypes.avro.data.record.RecordCompatibilityPositiveCases;
import org.hw.qe.schemaregistry.api.utils.compatibility.CompatibiltyPositiveNegativeTestCases;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Json for complex avro schema type.
 */
@Getter
@ToString
public class RecordSchemaType implements AvroSchemaType, SchemaTestDataInterface {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private static final ArrayList<Schema> SAMPLE_SCHEMAS = new ArrayList<>();

  /**
   * Get the list of sample records to be tested.
   * @return List of sample records to be tested
   */
  @Override
  @JsonIgnore
  public ArrayList<Schema> getPossibleDatatypeSchemas() {
    if (SAMPLE_SCHEMAS.isEmpty()) {
      for (Schema.Type primitiveDataType : PrimitiveSchemaType.values()) {
        Schema record = SchemaBuilder
                .record(
                    String.format("hwqe.registries.avro.complex.records_%s", primitiveDataType.name().toLowerCase()))
                .aliases("records")
                .fields()
                .name("field_" + primitiveDataType.getName())
                .type(primitiveDataType.name().toLowerCase())
                .noDefault()
                .name("field_with_default")
                .type("int")
                .withDefault(10)
                .endRecord();
        SAMPLE_SCHEMAS.add(record);
      }
    }
    return SAMPLE_SCHEMAS;
  }

  @Override
  public CompatibiltyPositiveNegativeTestCases getPositiveCasesForCompatibility() {
    return new RecordCompatibilityPositiveCases();
  }

  /**
   * Negative test cases for compatibility.
   * Test case 1 : Renaming a required field will not be backward compatible.
   * Test case 2 : Add a field with no default will not be backward compatible.
   * Test case 3 : Renaming a required field will not be forward compatible.
   */
  @Override
  public CompatibiltyPositiveNegativeTestCases getNegativeCasesForCompatibility() {
    return new RecordCompatibilityNegativeCases();
  }

  /**
   * Find a schema that is backward compatible to the given schema.
   * @return updated schema
   */
  @Override
  public Schema updateSchemaJson(Schema schemaToBeUpdated) {

    Schema.Field field = new Schema.Field(
            "new_field", SchemaBuilder.builder().intType(), "Newfield", 10);

    List<Schema.Field> fields = new ArrayList<>();
    fields.add(field);

    for (Schema.Field oldField : schemaToBeUpdated.getFields()) {
      Schema.Field newfield = new Schema.Field(
              oldField.name(), oldField.schema(), oldField.doc(), oldField.defaultVal());
      fields.add(newfield);
    }

    Schema record = Schema.createRecord(
            schemaToBeUpdated.getName(),
            "Changes",
            schemaToBeUpdated.getNamespace(),
            false,
            fields);
    log.debug("Schema:\n{}\nupdated to:\n{}",
            schemaToBeUpdated.toString(true), record.toString(true));
    return record;
  }

}
