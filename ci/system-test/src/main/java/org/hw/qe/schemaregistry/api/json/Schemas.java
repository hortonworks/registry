/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * POJO object for schemas.
 */
@Getter
@ToString
public class Schemas {
  @JsonProperty("entities")
  private List<SchemaMetadataInfo> entities = null;
}
