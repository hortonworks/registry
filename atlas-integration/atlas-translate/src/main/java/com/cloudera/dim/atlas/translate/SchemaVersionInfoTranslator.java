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

import com.cloudera.dim.atlas.types.MetadataEntityDef;
import com.cloudera.dim.atlas.types.SchemaVersionRelationshipDef;
import com.cloudera.dim.atlas.types.VersionEntityDef;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.cloudera.dim.atlas.types.VersionEntityDef.DESCRIPTION;
import static com.cloudera.dim.atlas.types.VersionEntityDef.ID;
import static com.cloudera.dim.atlas.types.VersionEntityDef.NAME;
import static com.cloudera.dim.atlas.types.VersionEntityDef.SCHEMA_TEXT;
import static com.cloudera.dim.atlas.types.VersionEntityDef.SCHEMA_VERSION_INFO;
import static com.cloudera.dim.atlas.types.VersionEntityDef.TIMESTAMP;
import static com.cloudera.dim.atlas.types.VersionEntityDef.VERSION;
import static com.google.common.base.Preconditions.checkNotNull;

public class SchemaVersionInfoTranslator implements AtlasTranslator<SchemaVersionInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaVersionInfoTranslator.class);

    @Override
    public AtlasEntity toAtlas(SchemaVersionInfo svi) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName(SCHEMA_VERSION_INFO);
        atlasEntity.setGuid(atlasEntity.getGuid());
        atlasEntity.setIsIncomplete(false);
        atlasEntity.setProvenanceType(0);
        //atlasEntity.setVersion(0);
        //atlasEntity.setProxy(false);
        Map<String, Object> attributes = new LinkedHashMap<>();
        atlasEntity.setAttributes(attributes);
        attributes.put(ID, svi.getId());
        attributes.put(NAME, svi.getName());
        attributes.put(VERSION, svi.getVersion());
        attributes.put(DESCRIPTION, svi.getDescription());
        attributes.put(SCHEMA_TEXT, svi.getSchemaText());
        attributes.put(TIMESTAMP, svi.getTimestamp());

        return atlasEntity;
    }

    @Override
    public SchemaVersionInfo fromAtlas(AtlasStruct atlasEntity) {
        return null;
    }

    public AtlasRelationship createRelationship(AtlasEntity meta, AtlasEntity versionInfo) {
        checkNotNull(meta, "meta");
        checkNotNull(versionInfo, "versionInfo");

        LOG.debug("Creating relationship between meta \"{}\" and version {}",
                meta.getAttribute(MetadataEntityDef.NAME), versionInfo.getAttribute(VersionEntityDef.VERSION));

        AtlasRelationship relationship = RelationshipHelper.createRelationship(meta, versionInfo, SchemaVersionRelationshipDef.RELATIONSHIP_NAME);
        relationship.setAttribute(ID, versionInfo.getAttribute(ID));
        relationship.setAttribute(VERSION, versionInfo.getAttribute(VERSION));
        return relationship;
    }
}
