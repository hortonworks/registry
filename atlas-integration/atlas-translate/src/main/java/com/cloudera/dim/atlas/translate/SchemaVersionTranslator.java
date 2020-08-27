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
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.state.InbuiltSchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.cloudera.dim.atlas.types.VersionEntityDef.*;
import static com.google.common.base.Preconditions.checkNotNull;

public class SchemaVersionTranslator implements AtlasTranslator<SchemaVersion> {

    private static Logger LOG = LoggerFactory.getLogger(SchemaVersionTranslator.class);

    private static final InbuiltSchemaVersionLifecycleState DEFAULT_VERSION_STATE = SchemaVersionLifecycleStates.INITIATED;

    @Override
    public AtlasEntity toAtlas(SchemaVersion schemaVersion) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName(SCHEMA_VERSION_INFO);
        atlasEntity.setGuid(atlasEntity.getGuid());
        atlasEntity.setIsIncomplete(false);
        atlasEntity.setProvenanceType(0);
        //atlasEntity.setVersion(0);
        //atlasEntity.setProxy(false);
        Map<String, Object> attributes = new LinkedHashMap<>();
        atlasEntity.setAttributes(attributes);
        attributes.put(DESCRIPTION, schemaVersion.getDescription());
        attributes.put(SCHEMA_TEXT, schemaVersion.getSchemaText());
        attributes.put(STATE, DEFAULT_VERSION_STATE.getId());
        attributes.put(TIMESTAMP, System.currentTimeMillis());

        return atlasEntity;
    }

    @Override
    public SchemaVersion fromAtlas(AtlasStruct atlasEntity) {
        return new SchemaVersion(
                (String)atlasEntity.getAttribute(SCHEMA_TEXT),
                (String)atlasEntity.getAttribute(DESCRIPTION),
                (Byte)atlasEntity.getAttribute(STATE)
        );
    }

    public AtlasEntity toAtlas(long versionId, SchemaVersion schemaVersion, SchemaMetadataInfo schemaMetadataInfo, String schemaName, int version, String fingerprint) {
        AtlasEntity atlasEntity = toAtlas(schemaVersion);

        Map<String, Object> attributes = atlasEntity.getAttributes();
        attributes.put(ID, versionId);
        attributes.put(NAME, schemaName);
        attributes.put(DESCRIPTION, schemaVersion.getDescription());
        attributes.put(SCHEMA_TEXT, schemaVersion.getSchemaText());
        attributes.put(FINGERPRINT, fingerprint);
        attributes.put(VERSION, version);
        attributes.put(TIMESTAMP, System.currentTimeMillis());
        attributes.put(STATE, DEFAULT_VERSION_STATE.getId());

        return atlasEntity;
    }

    public AtlasRelationship createRelationship(AtlasEntity meta, AtlasEntity versionInfo) {
        checkNotNull(meta, "meta");
        checkNotNull(versionInfo, "versionInfo");

        LOG.debug("Creating relationship between meta \"{}\" and version {}",
                meta.getAttribute(MetadataEntityDef.NAME), versionInfo.getAttribute(VersionEntityDef.VERSION));

        AtlasRelationship relationship = RelationshipHelper.createRelationship(meta, versionInfo, SchemaVersionRelationshipDef.RELATIONSHIP_NAME);
        relationship.setAttribute(ID, versionInfo.getAttribute(ID));
        relationship.setAttribute(VERSION, versionInfo.getAttribute(VERSION));
        relationship.setAttribute(FINGERPRINT, versionInfo.getAttribute(FINGERPRINT));
        return relationship;
    }

}
