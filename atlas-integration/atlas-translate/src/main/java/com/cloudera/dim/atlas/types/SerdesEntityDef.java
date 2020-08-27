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

public class SerdesEntityDef extends AtlasEntityDef implements SchemaRegistryServiceType {

    public static final String SCHEMA_SERDES_INFO = "schema_serdes_info"+TODORemoveThis.COUNTER;

    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String TIMESTAMP = "timestamp";
    public static final String FILE_ID = "fileId";
    public static final String SERIALIZER_CLASS_NAME = "serializerClassName";
    public static final String DESERIALIZER_CLASS_NAME = "deserializerClassName";

    SerdesEntityDef() {
        setName(SCHEMA_SERDES_INFO);
        setServiceType(SERVICE_TYPE);
        setTypeVersion(TYPE_VERSION);

        AtlasAttributeDef id = new AtlasAttributeDef(ID, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);
        id.setIsUnique(true);
        AtlasAttributeDef description = new AtlasAttributeDef(DESCRIPTION, ATLAS_TYPE_STRING, true, Cardinality.SINGLE);
        AtlasAttributeDef name = new AtlasAttributeDef(NAME, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        name.setIsIndexable(true);
        AtlasAttributeDef fileId = new AtlasAttributeDef(FILE_ID, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        AtlasAttributeDef serializerClassName = new AtlasAttributeDef(SERIALIZER_CLASS_NAME, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        AtlasAttributeDef deserializerClassName = new AtlasAttributeDef(DESERIALIZER_CLASS_NAME, ATLAS_TYPE_STRING, false, Cardinality.SINGLE);
        AtlasAttributeDef timestamp = new AtlasAttributeDef(TIMESTAMP, ATLAS_TYPE_LONG, false, Cardinality.SINGLE);

        setAttributeDefs(newArrayList(
                id, description, name, fileId, serializerClassName, deserializerClassName, timestamp
        ));
    }
}
