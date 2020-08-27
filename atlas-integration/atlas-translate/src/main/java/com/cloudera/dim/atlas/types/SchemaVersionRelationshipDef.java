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
package com.cloudera.dim.atlas.types;

import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;

import static com.cloudera.dim.atlas.types.VersionEntityDef.FINGERPRINT;
import static com.cloudera.dim.atlas.types.VersionEntityDef.ID;
import static com.cloudera.dim.atlas.types.VersionEntityDef.VERSION;
import static com.google.common.collect.Lists.newArrayList;

public class SchemaVersionRelationshipDef extends AtlasRelationshipDef implements SchemaRegistryServiceType {

    public static final String RELATIONSHIP_NAME = "schema_version"+TODORemoveThis.COUNTER;
    public static final String RELATIONSHIP_LABEL = "meta.version";

    public static final String REL_ONE = "versions";  // rel.name from the PoV of one
    public static final String REL_MANY = "schema_meta";  // rel.name from the PoV of many

    SchemaVersionRelationshipDef() {
        setName(RELATIONSHIP_NAME);
        setServiceType(SERVICE_TYPE);
        setTypeVersion(TYPE_VERSION);
        setRelationshipCategory(RelationshipCategory.AGGREGATION);
        setRelationshipLabel(RELATIONSHIP_LABEL);
        setPropagateTags(PropagateTags.NONE);

        setEndDef1(new AtlasRelationshipEndDef(
                MetadataEntityDef.SCHEMA_METADATA_INFO,
                REL_ONE,
                Cardinality.SET, true, true
        ));
        setEndDef2(new AtlasRelationshipEndDef(
                VersionEntityDef.SCHEMA_VERSION_INFO,
                REL_MANY,
                Cardinality.SINGLE, false, true
        ));

        AtlasAttributeDef schemaMetadataId = new AtlasAttributeDef(ID, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);
        schemaMetadataId.setIsUnique(true);
        AtlasAttributeDef version = new AtlasAttributeDef(VERSION, ATLAS_TYPE_INT, false, Cardinality.SINGLE);
        AtlasAttributeDef fingerprint = new AtlasAttributeDef(FINGERPRINT, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        setAttributeDefs(newArrayList(schemaMetadataId, version, fingerprint));
    }
}
