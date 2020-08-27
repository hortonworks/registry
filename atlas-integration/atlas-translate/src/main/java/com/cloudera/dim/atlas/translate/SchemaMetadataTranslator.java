/**
 * Copyright 2016-2020 Cloudera, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.cloudera.dim.atlas.translate;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.cloudera.dim.atlas.types.MetadataEntityDef.*;
import static com.google.common.base.Preconditions.checkNotNull;

public class SchemaMetadataTranslator implements AtlasTranslator<SchemaMetadata> {

    @Override
    public AtlasEntity toAtlas(SchemaMetadata schemaMetadata) {
        AtlasEntity atlasEntity = this.toAtlas(schemaMetadata, 0L);
        atlasEntity.setAttribute(SCHEMA_METADATA_ID, atlasEntity.getGuid());
        return atlasEntity;
    }

    public AtlasEntity toAtlas(SchemaMetadata schemaMetadata, long id) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName(SCHEMA_METADATA_INFO);
        //atlasEntity.setGuid(nextInternalId());
        atlasEntity.setIsIncomplete(false);
        atlasEntity.setProvenanceType(0);
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put(SCHEMA_METADATA_ID, id);
        attributes.put(NAME, schemaMetadata.getName());
        attributes.put(SCHEMA_GROUP, schemaMetadata.getSchemaGroup());
        attributes.put(DESCRIPTION, schemaMetadata.getDescription());
        attributes.put(TYPE, schemaMetadata.getType());
        attributes.put(COMPATIBILITY, schemaMetadata.getCompatibility().name());
        if (schemaMetadata.getValidationLevel() == null) {
            attributes.put(VALIDATION_LEVEL, SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL.name());
        } else {
            attributes.put(VALIDATION_LEVEL, schemaMetadata.getValidationLevel().name());
        }
        attributes.put(EVOLVE, schemaMetadata.isEvolve());
        attributes.put(TIMESTAMP, System.currentTimeMillis());
        atlasEntity.setAttributes(attributes);

        return atlasEntity;
    }

    @Override
    public SchemaMetadata fromAtlas(AtlasStruct metaEntity) {
        return new SchemaMetadata.Builder((String) metaEntity.getAttribute(NAME))
                .type((String) metaEntity.getAttribute(TYPE))
                .schemaGroup((String) metaEntity.getAttribute(SCHEMA_GROUP))
                .description((String) metaEntity.getAttribute(DESCRIPTION))
                .compatibility(SchemaCompatibility.valueOf((String) metaEntity.getAttribute(COMPATIBILITY)))
                .validationLevel(SchemaValidationLevel.valueOf((String) metaEntity.getAttribute(VALIDATION_LEVEL)))
                .evolve((Boolean)metaEntity.getAttribute(EVOLVE))
                .build();
    }

    public SchemaMetadataInfo fromAtlasIntoInfo(AtlasStruct metaEntity) {
        checkNotNull(metaEntity, "meta AtlasEntity was null");
        SchemaMetadata metadata = this.fromAtlas(metaEntity);

        return new SchemaMetadataInfo(metadata,
                ((Number) metaEntity.getAttribute(SCHEMA_METADATA_ID)).longValue(),
                ((Number) metaEntity.getAttribute(TIMESTAMP)).longValue());
    }

    public void updateEntity(AtlasEntity metaEntity, SchemaMetadata schemaMetadata) {
        metaEntity.setAttribute(SCHEMA_GROUP, schemaMetadata.getSchemaGroup());
        metaEntity.setAttribute(DESCRIPTION, schemaMetadata.getDescription());
        metaEntity.setAttribute(TYPE, schemaMetadata.getType());
        metaEntity.setAttribute(COMPATIBILITY, schemaMetadata.getCompatibility().name());
        if (schemaMetadata.getValidationLevel() != null) {
            metaEntity.setAttribute(VALIDATION_LEVEL, schemaMetadata.getValidationLevel().name());
        }
        metaEntity.setAttribute(EVOLVE, schemaMetadata.isEvolve());
    }
}
