/*
 * Copyright  (c) 2011-2020, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Suppliers;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import lombok.Getter;
import lombok.ToString;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.SchemaTestDataInterface;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordCompatibilityNegativeCases;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record.RecordCompatibilityPositiveCases;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.compatibility.CompatibiltyPositiveNegativeTestCases;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Json for complex avro schema type.
 */
@Getter
@ToString
public class RecordSchemaType implements AvroSchemaType, SchemaTestDataInterface<Schema> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final Supplier<ArrayList<Schema>> SAMPLE_SCHEMAS = Suppliers.memoize(() -> {
        final ArrayList<Schema> sampleSchemas = new ArrayList<>();
        for (Schema.Type primitiveDataType : PrimitiveSchemaType.values()) {
            Schema record = SchemaBuilder
                    .record(String.format("hwqe.registries.avro.complex.records_%s", primitiveDataType.name().toLowerCase()))
                    .aliases("records")
                    .fields()
                    .name("field_" + primitiveDataType.getName())
                    .type(primitiveDataType.name().toLowerCase())
                    .noDefault()
                    .name("field_with_default")
                    .type("int")
                    .withDefault(10)
                    .endRecord();
            sampleSchemas.add(record);
        }
        return sampleSchemas;
    });

    /**
     * Get the list of sample records to be tested.
     *
     * @return List of sample records to be tested
     */
    @Override
    @JsonIgnore
    public ArrayList<Schema> getPossibleDatatypeSchemas() {
        return SAMPLE_SCHEMAS.get();
    }

    @Override
    public CompatibiltyPositiveNegativeTestCases getPositiveCasesForCompatibility(ISchemaRegistryClient schemaRegistryClient) {
        return new RecordCompatibilityPositiveCases(schemaRegistryClient);
    }

    /**
     * Negative test cases for compatibility.
     * Test case 1 : Renaming a required field will not be backward compatible.
     * Test case 2 : Add a field with no default will not be backward compatible.
     * Test case 3 : Renaming a required field will not be forward compatible.
     */
    @Override
    public CompatibiltyPositiveNegativeTestCases getNegativeCasesForCompatibility(ISchemaRegistryClient schemaRegistryClient) {
        return new RecordCompatibilityNegativeCases(schemaRegistryClient);
    }

    /**
     * Find a schema that is backward compatible to the given schema.
     *
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
