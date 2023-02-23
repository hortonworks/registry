/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.cloudera.dim.schemaregistry.testcontainers.tests;

import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.SchemaRegistryHelper.SchemametadataFields;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AvroSchemaMetadataTest extends TestcontainersTestsBase {

    private static Logger LOG = LoggerFactory.getLogger(AvroSchemaMetadataTest.class);

    @Test
    public void testChangeSchemaDescription() {
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Registering schema");
        Long schemaID = schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        SchemaMetadata updatedschemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Updating schema description");
        schemaRegistryClient.updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());
        Assertions.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata,
                "Schema metadata after change");
        Assertions.assertEquals(schemaID, schemaMetadataInfo.getId(),
                "Schema ID changed when trying to change description");
    }


    @Test
    @Disabled("This was skipped at ci tests. Check before re-enable.")
    public void testChangeSchemaName() {
        SchemaMetadata originalSchemaMetadata = new SchemaMetadata.Builder(getSchemaName())
                .type(schemaRegistryHelper.DEFAULT_SCHEMA_TYPE)
                .build();
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);
        SchemaMetadata updatedschemaMetadata = new SchemaMetadata.Builder(getSchemaName() + "_updated")
                .type(schemaRegistryHelper.DEFAULT_SCHEMA_TYPE)
                .build();
        schemaRegistryClient.updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
        LOG.error("Failure with schema name changed caused due to BUG-88198");
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());
        Assertions.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
                "change");
    }


    @Test
    @Disabled("This was skipped at ci tests. Check before re-enable.")
    public void testChangeSchemaType() {
        SchemaMetadata originalSchemaMetadata = new SchemaMetadata.Builder(getSchemaName())
                .type(schemaRegistryHelper.DEFAULT_SCHEMA_TYPE)
                .build();
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        SchemaMetadata updatedschemaMetadata = new SchemaMetadata.Builder(getSchemaName())
                .type("foo")
                .build();
        LOG.error("Check BUG-88200 for " + getTestName());
        schemaRegistryClient.updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());
        Assertions.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
                "change");
    }


    @Test
    public void testSchemaWithInvalidSchemaType() {
        LOG.error("Check BUG-88200 for " + getTestName());
        schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(), "Registering for test " + getTestName(),
                Pair.of(SchemametadataFields.TYPE, "anc"));
    }


    @Test
    public void testChangingSchemaGroup() {
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(
                getSchemaName(), "Registering for test " + getTestName());
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        SchemaMetadata updatedschemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(
                getSchemaName(), "Registering for test " + getTestName(),
                Pair.of(SchemametadataFields.SCHEMAGROUP, "RandomGroup"));
        LOG.error("Check BUG-88200 for " + getTestName());
        schemaRegistryClient.updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());
        Assertions.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata,
                "Schema metadata after change");
    }


    @Test
    public void testChangingSchemaCompatibility() {
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "Version1", Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.BACKWARD));
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        SchemaMetadata updatedschemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(getSchemaName(),
                "New updated schema metdata",
                Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.NONE));
        schemaRegistryClient.updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());

        Assertions.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata,
                "Schema metadata after change");
    }


    @Test
    public void testChangingValiditationLevel() {
        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(
                getSchemaName(), "Registering for test " + getTestName(),
                Pair.of(SchemametadataFields.VALIDATIONLEVEL, SchemaValidationLevel.ALL));
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        SchemaMetadata updatedschemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(
                getSchemaName(), "Version2",
                Pair.of(SchemametadataFields.VALIDATIONLEVEL, SchemaValidationLevel.LATEST));
        schemaRegistryClient.updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());

        Assertions.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata,
                "Schema metadata after change");
    }


    @Test
    public void testTogglingEvolveOfSchema() {

        SchemaMetadata originalSchemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(
                getSchemaName(),
                "Registering for test " + getTestName(),
                Pair.of(SchemametadataFields.EVOLVE, true));
        schemaRegistryClient.addSchemaMetadata(originalSchemaMetadata);

        SchemaMetadata updatedschemaMetadata = schemaRegistryHelper.getSchemaMetadataWithDefault(
                getSchemaName(),
                "New updated schema metdata",
                Pair.of(SchemametadataFields.EVOLVE, false));
        schemaRegistryClient.updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistryClient.getSchemaMetadataInfo(getSchemaName());

        Assertions.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata,
                "Schema metadata after change");
    }


}
