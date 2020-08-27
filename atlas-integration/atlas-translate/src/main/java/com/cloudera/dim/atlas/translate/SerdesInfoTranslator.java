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
import com.cloudera.dim.atlas.types.MetadataEntityDef;

import com.cloudera.dim.atlas.types.SerdesMappingRelationshipDef;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.cloudera.dim.atlas.types.SerdesEntityDef.DESCRIPTION;
import static com.cloudera.dim.atlas.types.SerdesEntityDef.DESERIALIZER_CLASS_NAME;
import static com.cloudera.dim.atlas.types.SerdesEntityDef.FILE_ID;
import static com.cloudera.dim.atlas.types.SerdesEntityDef.ID;
import static com.cloudera.dim.atlas.types.SerdesEntityDef.NAME;
import static com.cloudera.dim.atlas.types.SerdesEntityDef.SCHEMA_SERDES_INFO;
import static com.cloudera.dim.atlas.types.SerdesEntityDef.SERIALIZER_CLASS_NAME;
import static com.cloudera.dim.atlas.types.SerdesEntityDef.TIMESTAMP;
import static com.google.common.base.Preconditions.checkNotNull;

public class SerdesInfoTranslator implements AtlasTranslator<SerDesInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(SerdesInfoTranslator.class);

    @Override
    public AtlasEntity toAtlas(SerDesInfo serdes) {
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName(SCHEMA_SERDES_INFO);
        atlasEntity.setIsIncomplete(false);
        atlasEntity.setProvenanceType(0);
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put(ID, serdes.getId());
        attributes.put(NAME, serdes.getSerDesPair().getName());
        attributes.put(DESCRIPTION, serdes.getSerDesPair().getDescription());
        attributes.put(FILE_ID, serdes.getSerDesPair().getFileId());
        attributes.put(SERIALIZER_CLASS_NAME, serdes.getSerDesPair().getSerializerClassName());
        attributes.put(DESERIALIZER_CLASS_NAME, serdes.getSerDesPair().getDeserializerClassName());
        attributes.put(TIMESTAMP, serdes.getTimestamp());
        atlasEntity.setAttributes(attributes);

        return atlasEntity;
    }

    @Override
    public SerDesInfo fromAtlas(AtlasStruct atlasEntity) {
        SerDesPair serDesPair = new SerDesPair(
                (String) atlasEntity.getAttribute(NAME),
                (String) atlasEntity.getAttribute(DESCRIPTION),
                (String) atlasEntity.getAttribute(FILE_ID),
                (String) atlasEntity.getAttribute(SERIALIZER_CLASS_NAME),
                (String) atlasEntity.getAttribute(DESERIALIZER_CLASS_NAME));
        return new SerDesInfo(
                ((Number) atlasEntity.getAttribute(ID)).longValue(),
                ((Number) atlasEntity.getAttribute(TIMESTAMP)).longValue(),
                serDesPair
        );
    }

    public AtlasRelationship createRelationship(AtlasEntity meta, AtlasEntity serdes) {
        checkNotNull(meta, "meta");
        checkNotNull(serdes, "serdes");

        LOG.debug("Creating relationship between meta \"{}\" and serdes \"{}\"",
                meta.getAttribute(MetadataEntityDef.NAME), serdes.getAttribute(BranchEntityDef.NAME));

        return RelationshipHelper.createRelationship(meta, serdes, SerdesMappingRelationshipDef.RELATIONSHIP_NAME);
    }

}
