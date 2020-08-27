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

public class VersionEntityDef extends AtlasEntityDef implements SchemaRegistryServiceType {

    public static final String SCHEMA_VERSION_INFO = "schema_version_info"+TODORemoveThis.COUNTER;

    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String SCHEMA_TEXT = "schemaText";
    public static final String FINGERPRINT = "fingerprint";
    public static final String VERSION = "version";
    public static final String STATE = "state";
    public static final String TIMESTAMP = "timestamp";

    VersionEntityDef() {
        setName(SCHEMA_VERSION_INFO);
        setServiceType(SERVICE_TYPE);
        setTypeVersion(TYPE_VERSION);

        AtlasAttributeDef schemaMetadataId = new AtlasAttributeDef(ID, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);
        schemaMetadataId.setIsUnique(true);
        AtlasAttributeDef name = new AtlasAttributeDef(NAME, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        name.setIsIndexable(true);
        AtlasAttributeDef description = new AtlasAttributeDef(DESCRIPTION, ATLAS_TYPE_STRING, true, Cardinality.SINGLE);
        AtlasAttributeDef schemaText = new AtlasAttributeDef(SCHEMA_TEXT, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        AtlasAttributeDef fingerprint = new AtlasAttributeDef(FINGERPRINT, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        fingerprint.setIsIndexable(true);
        AtlasAttributeDef version = new AtlasAttributeDef(VERSION, ATLAS_TYPE_INT, false, Cardinality.SINGLE);
        AtlasAttributeDef timestamp = new AtlasAttributeDef(TIMESTAMP, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);

        setAttributeDefs(newArrayList(
                schemaMetadataId, name, description, schemaText, fingerprint, version, timestamp
        ));
    }
}
