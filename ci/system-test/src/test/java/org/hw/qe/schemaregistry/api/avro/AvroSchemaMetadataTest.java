/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.avro;
/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import org.apache.commons.lang3.tuple.Pair;
import org.hw.qe.schemaregistry.api.BaseAPITest;
import org.hw.qe.schemaregistry.api.utils.SchemaRegistryHelper.SchemametadataFields;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for creating schema registry.
 */
@Test
public class AvroSchemaMetadataTest extends BaseAPITest {
  /**
   * Test to change schema description.
   */
  @Test
  public void testChangeSchemaDescription() {
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Registering schema");
    Long schemaID = getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    SchemaMetadata updatedschemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Updating schema description");
    getSchemaRegistryClient().updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());
    Assert.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
        "change");
    Assert.assertEquals(schemaID, schemaMetadataInfo.getId(), "Schema ID changed when trying to change description");
  }

  /**
   * Test to change schema name.
   */
  @Test(enabled = false)
  public void testChangeSchemaName() {
    SchemaMetadata originalSchemaMetadata = new SchemaMetadata.Builder(getSchemaName())
        .type(getSchemaRegistryHelper().DEFAULT_SCHEMA_TYPE)
        .build();
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);
    SchemaMetadata updatedschemaMetadata = new SchemaMetadata.Builder(getSchemaName() + "_updated")
        .type(getSchemaRegistryHelper().DEFAULT_SCHEMA_TYPE)
        .build();
    getSchemaRegistryClient().updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
    getLog().error("Failure with schema name changed caused due to BUG-88198");
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());
    Assert.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
        "change");
  }

  /**
   * Test to change schema type to an invalid value.
   */
  @Test(enabled = false)
  public void testChangeSchemaType() {
    SchemaMetadata originalSchemaMetadata = new SchemaMetadata.Builder(getSchemaName())
        .type(getSchemaRegistryHelper().DEFAULT_SCHEMA_TYPE)
        .build();
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    SchemaMetadata updatedschemaMetadata = new SchemaMetadata.Builder(getSchemaName())
        .type("foo")
        .build();
    getLog().error("Check BUG-88200 for " + getTestName());
    getSchemaRegistryClient().updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());
    Assert.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
        "change");
  }

  /**
   * Test to add schema with invalid schema type.
   */
  @Test
  public void testSchemaWithInvalidSchemaType() {
    getLog().error("Check BUG-88200 for " + getTestName());
    getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(), "Registering for test " + getTestName(),
        Pair.of(SchemametadataFields.TYPE, "anc"));
  }

  /**
   * Test changing schema group.
   */
  @Test
  public void testChangingSchemaGroup() {
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(
        getSchemaName(), "Registering for test " + getTestName());
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    SchemaMetadata updatedschemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(
        getSchemaName(), "Registering for test " + getTestName(),
        Pair.of(SchemametadataFields.SCHEMAGROUP, "RandomGroup"));
    getLog().error("Check BUG-88200 for " + getTestName());
    getSchemaRegistryClient().updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());
    Assert.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata,
        "Schema metadata after change");
  }

  /**
   * Test changing compatibility.
   */
  @Test
  public void testChangingSchemaCompatibility() {
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "Version1", Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.BACKWARD));
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    SchemaMetadata updatedschemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(getSchemaName(),
        "New updated schema metdata",
        Pair.of(SchemametadataFields.COMPATIBILITY, SchemaCompatibility.NONE));
    getSchemaRegistryClient().updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());

    Assert.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
        "change");
  }

  /**
   * Test changing validation level.
   */
  @Test
  public void testChangingValiditationLevel() {
    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(
        getSchemaName(), "Registering for test " + getTestName(),
        Pair.of(SchemametadataFields.VALIDATIONLEVEL, SchemaValidationLevel.ALL));
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    SchemaMetadata updatedschemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(
        getSchemaName(), "Version2",
        Pair.of(SchemametadataFields.VALIDATIONLEVEL, SchemaValidationLevel.LATEST));
    getSchemaRegistryClient().updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());

    Assert.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
        "change");
  }

  /**
   * Test toggling evolve.
   */
  @Test
  public void testTogglingEvolveOfSchema() {

    SchemaMetadata originalSchemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(
        getSchemaName(),
        "Registering for test " + getTestName(),
        Pair.of(SchemametadataFields.EVOLVE, true));
    getSchemaRegistryClient().addSchemaMetadata(originalSchemaMetadata);

    SchemaMetadata updatedschemaMetadata = getSchemaRegistryHelper().getSchemaMetadataWithDefault(
        getSchemaName(),
        "New updated schema metdata",
        Pair.of(SchemametadataFields.EVOLVE, false));
    getSchemaRegistryClient().updateSchemaMetadata(getSchemaName(), updatedschemaMetadata);
    SchemaMetadataInfo schemaMetadataInfo = getSchemaRegistryClient().getSchemaMetadataInfo(getSchemaName());

    Assert.assertEquals(schemaMetadataInfo.getSchemaMetadata(), updatedschemaMetadata, "Schema metadata after " +
        "change");
  }
}
