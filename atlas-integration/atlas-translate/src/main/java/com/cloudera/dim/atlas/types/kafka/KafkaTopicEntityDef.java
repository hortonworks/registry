/**
 * Copyright 2016-2021 Cloudera, Inc.
 * <p>
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
package com.cloudera.dim.atlas.types.kafka;

import com.cloudera.dim.atlas.types.SchemaRegistryServiceType;
import org.apache.atlas.model.typedef.AtlasEntityDef;

import static com.google.common.collect.Lists.newArrayList;

public class KafkaTopicEntityDef extends AtlasEntityDef implements SchemaRegistryServiceType {

    public static final String KAFKA_TOPIC = "kafka_topic";

    public static final String NAME = "name";

    KafkaTopicEntityDef() {
        setName(KAFKA_TOPIC);
        setServiceType(SERVICE_TYPE);
        setTypeVersion(TYPE_VERSION);

        AtlasAttributeDef name = new AtlasAttributeDef(NAME, ATLAS_TYPE_STRING, false, AtlasAttributeDef.Cardinality.SINGLE);
        name.setIsUnique(true);
        name.setIsIndexable(true);

        setAttributeDefs(newArrayList(name));
    }

}
