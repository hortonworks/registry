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

import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;

import static com.google.common.collect.Lists.newArrayList;

public class MetadataEntityDef extends AtlasEntityDef implements SchemaRegistryServiceType {

    public static final String SCHEMA_METADATA_INFO = "schema_metadata_info"+TODORemoveThis.COUNTER;

    public static final String SCHEMA_METADATA_ID = "schemaMetadataId";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String SCHEMA_GROUP = "schemaGroup";
    public static final String COMPATIBILITY = "compatibility";
    public static final String VALIDATION_LEVEL = "validationLevel";
    public static final String DESCRIPTION = "description";
    public static final String EVOLVE = "evolve";
    public static final String TIMESTAMP = "timestamp";

    MetadataEntityDef() {
        setName(SCHEMA_METADATA_INFO);
        setServiceType(SERVICE_TYPE);
        setTypeVersion(TYPE_VERSION);

        AtlasAttributeDef schemaMetadataId = new AtlasAttributeDef(SCHEMA_METADATA_ID, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);
        schemaMetadataId.setIsUnique(true);
        AtlasAttributeDef name = new AtlasAttributeDef(NAME, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        name.setIsUnique(true);
        name.setIsIndexable(true);
        AtlasAttributeDef type = new AtlasAttributeDef(TYPE, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        type.setIsIndexable(true);
        AtlasAttributeDef schemaGroup = new AtlasAttributeDef(SCHEMA_GROUP, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        schemaGroup.setIsIndexable(true);
        AtlasAttributeDef compatibility = new AtlasAttributeDef(COMPATIBILITY, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        AtlasAttributeDef validationLevel = new AtlasAttributeDef(VALIDATION_LEVEL, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        AtlasAttributeDef description = new AtlasAttributeDef(DESCRIPTION, ATLAS_TYPE_STRING, true, Cardinality.SINGLE);
        AtlasAttributeDef evolve = new AtlasAttributeDef(EVOLVE, ATLAS_TYPE_BOOLEAN, false, Cardinality.SINGLE);
        AtlasAttributeDef timestamp = new AtlasAttributeDef(TIMESTAMP, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);

        setAttributeDefs(newArrayList(
                schemaMetadataId, name, type, schemaGroup, compatibility, validationLevel, description, evolve, timestamp
        ));
    }
}
