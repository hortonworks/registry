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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;

import static com.google.common.base.Preconditions.checkNotNull;

public class RelationshipHelper {

    public static AtlasRelationship createRelationship(AtlasEntity end1, AtlasEntity end2, String name) {
        checkNotNull(end1, "end1");
        checkNotNull(end2, "end2");

        AtlasRelationship relationship = new AtlasRelationship();
        relationship.setEnd1(new AtlasObjectId(end1.getGuid()));
        relationship.setEnd2(new AtlasObjectId(end2.getGuid()));
        relationship.setTypeName(name);

        return relationship;
    }
}
