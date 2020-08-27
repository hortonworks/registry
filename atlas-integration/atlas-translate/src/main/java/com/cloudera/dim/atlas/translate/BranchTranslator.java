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

import com.cloudera.dim.atlas.types.BranchEntityDef;
import com.cloudera.dim.atlas.types.VersionBranchRelationshipDef;
import com.cloudera.dim.atlas.types.MetaBranchRelationshipDef;
import com.cloudera.dim.atlas.types.MetadataEntityDef;
import com.cloudera.dim.atlas.types.VersionEntityDef;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.cloudera.dim.atlas.types.BranchEntityDef.DESCRIPTION;
import static com.cloudera.dim.atlas.types.BranchEntityDef.ID;
import static com.cloudera.dim.atlas.types.BranchEntityDef.NAME;
import static com.cloudera.dim.atlas.types.BranchEntityDef.SCHEMA_BRANCH;
import static com.cloudera.dim.atlas.types.BranchEntityDef.SCHEMA_METADATA_NAME;
import static com.cloudera.dim.atlas.types.BranchEntityDef.TIMESTAMP;
import static com.google.common.base.Preconditions.checkNotNull;

public class BranchTranslator implements AtlasTranslator<SchemaBranch> {

    private static final Logger LOG = LoggerFactory.getLogger(BranchTranslator.class);

    @Override
    public AtlasEntity toAtlas(SchemaBranch branch) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName(SCHEMA_BRANCH);
        atlasEntity.setIsIncomplete(false);
        atlasEntity.setProvenanceType(0);
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put(ID, branch.getId());
        attributes.put(NAME, branch.getName());
        attributes.put(SCHEMA_METADATA_NAME, branch.getSchemaMetadataName());
        attributes.put(DESCRIPTION, branch.getDescription());
        attributes.put(TIMESTAMP, branch.getTimestamp());
        atlasEntity.setAttributes(attributes);

        return atlasEntity;
    }

    @Override
    public SchemaBranch fromAtlas(AtlasStruct atlasEntity) {
        return new SchemaBranch(
                ((Number)atlasEntity.getAttribute(ID)).longValue(),
                (String)atlasEntity.getAttribute(NAME),
                (String)atlasEntity.getAttribute(SCHEMA_METADATA_NAME),
                (String)atlasEntity.getAttribute(DESCRIPTION),
                ((Number)atlasEntity.getAttribute(TIMESTAMP)).longValue());
    }

    public AtlasRelationship createRelationshipWithMeta(AtlasEntity meta, AtlasEntity branch) {
        checkNotNull(meta, "meta");
        checkNotNull(branch, "branch");

        LOG.debug("Creating relationship between meta \"{}\" and branch \"{}\"",
                meta.getAttribute(MetadataEntityDef.NAME), branch.getAttribute(BranchEntityDef.NAME));

        AtlasRelationship relationship = RelationshipHelper.createRelationship(meta, branch, MetaBranchRelationshipDef.RELATIONSHIP_NAME);
        relationship.setAttribute(BranchEntityDef.NAME, branch.getAttribute(BranchEntityDef.NAME));
        return relationship;
    }

    public AtlasRelationship createRelationshipWithVersion(AtlasEntity version, AtlasEntity branch) {
        checkNotNull(version, "version");
        checkNotNull(branch, "branch");

        LOG.debug("Creating relationship between version \"{}\" and branch \"{}\"",
                version.getAttribute(VersionEntityDef.ID), branch.getAttribute(BranchEntityDef.NAME));

        AtlasRelationship relationship = RelationshipHelper.createRelationship(version, branch, VersionBranchRelationshipDef.RELATIONSHIP_NAME);
        relationship.setAttribute(BranchEntityDef.NAME, branch.getAttribute(BranchEntityDef.NAME));
        return relationship;
    }
}
