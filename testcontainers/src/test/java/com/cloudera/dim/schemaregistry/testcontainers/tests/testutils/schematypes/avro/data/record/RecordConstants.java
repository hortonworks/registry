/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.schematypes.avro.data.record;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.tuple.Pair;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.SchemaRegistryHelper.SchemametadataFields;

/**
 * Class with constant objects for record type.
 */
final class RecordConstants {
    static final Pair SCHEMA_WITH_BACKWARD_COMPATIBILITY =
            Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.BACKWARD);

    static final Pair SCHEMA_WITH_FORWARD_COMPATIBILITY =
            Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.FORWARD);

    static final Pair SCHEMA_WITH_NONE_COMPATIBILITY =
            Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.NONE);

    static final Pair SCHEMA_WITH_BOTH_COMPATIBILITY =
            Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.BOTH);

    static final String SCHEMA_WITH_REQUIRED_AND_OPTIONAL_FIELD = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_int")
            .type("int")
            .noDefault()
            .name("field_with_default")
            .type("int")
            .withDefault(10)
            .endRecord()
            .toString(true);

    static final String RANDOM_STRING = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.random")
            .fields()
            .name("firstname")
            .type("string")
            .noDefault()
            .name("lastname")
            .type("string")
            .withDefault("bhat")
            .endRecord()
            .toString(true);

    static final String SCHEMA_WITH_LONG_REQUIRED_FIELD = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_int")
            .type("long")
            .noDefault()
            .endRecord()
            .toString(true);

    static final String SCHEMA_WITH_OPTIONAL_FIELD_ONLY = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_with_default")
            .type("int")
            .withDefault(10)
            .endRecord()
            .toString(true);

    static final String SCHEMA_WITH_REQUIRED_FIELD_RENAMED = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_int_renamed")
            .type("int")
            .noDefault()
            .endRecord()
            .toString(true);

    static final String SCHEMA_WITH_OPTIONAL_FIELD_RENAMED = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_with_defaultrenamed")
            .type("int")
            .withDefault(10)
            .endRecord()
            .toString(true);

    static final String SCHEMA_WITH_REQUIRED_FIELD_ONLY = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_int")
            .type("int")
            .noDefault()
            .endRecord()
            .toString(true);

    static final String SCHEMA_WITH_REQUIRED_FIELD_MADE_OPTIONAL = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_int")
            .type("int")
            .withDefault(10)
            .endRecord()
            .toString(true);

    static final String SCHEMA_WITH_OPTIONAL_FIELD_MADE_REQUIRED = SchemaBuilder
            .record("hwqe.registries.avro.complex.records.compatibility")
            .fields()
            .name("field_with_default")
            .type("int")
            .noDefault()
            .endRecord()
            .toString(true);

    /**
     * Private constructor for a utility class.
     */
    private RecordConstants() {

    }
}
