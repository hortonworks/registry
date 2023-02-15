/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro;

import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import org.apache.avro.Schema;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.SchemaTypesInterface;

/**
 * Avro schema type implementation of schema types.
 */
public interface AvroSchemaType extends SchemaTypesInterface<Schema> {


    @Override
    default String getType() {
        return AvroSchemaProvider.TYPE;
    }

    /**
     * Get json form of the schema.
     *
     * @param schema Schema to be got
     * @return Json form of the schema
     */
    @Override
    default String getJson(Schema schema) {
        return schema.toString(true);
    }
}
