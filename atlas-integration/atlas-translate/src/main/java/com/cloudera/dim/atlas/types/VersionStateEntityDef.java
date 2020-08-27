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
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;

import static com.google.common.collect.Lists.newArrayList;

public class VersionStateEntityDef extends AtlasEntityDef implements SchemaRegistryServiceType {

    public static final String SCHEMA_VERSION_STATE = "schema_version_state"+TODORemoveThis.COUNTER;

    public static final String TIMESTAMP = "timestamp";
    public static final String ID = "id";
    public static final String SCHEMA_VERSION_ID = "schemaVersionId";
    public static final String STATE_ID = "stateId";
    public static final String SEQUENCE = "sequence";
    public static final String DETAILS = "details";

    VersionStateEntityDef() {
        setName(SCHEMA_VERSION_STATE);
        setServiceType(SERVICE_TYPE);
        setTypeVersion(TYPE_VERSION);

        AtlasAttributeDef id = new AtlasAttributeDef(ID, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);
        AtlasAttributeDef schemaVersionId = new AtlasAttributeDef(SCHEMA_VERSION_ID, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);
        AtlasAttributeDef stateId = new AtlasAttributeDef(STATE_ID, ATLAS_TYPE_SHORT, false, Cardinality.SINGLE);
        AtlasAttributeDef sequence = new AtlasAttributeDef(SEQUENCE, ATLAS_TYPE_INT, false, Cardinality.SINGLE);
        AtlasAttributeDef details = new AtlasAttributeDef(DETAILS, ATLAS_TYPE_STRING, true, Cardinality.SINGLE);
        AtlasAttributeDef timestamp = new AtlasAttributeDef(TIMESTAMP, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);

        setAttributeDefs(newArrayList(
                id, schemaVersionId, stateId, sequence, details, timestamp
        ));
    }
}
