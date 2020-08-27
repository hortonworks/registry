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

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/** The entire SchemaRegistry model mapped in Atlas typedefs. */
public class Model extends AtlasTypesDef {

    public Model() {
        setEntityDefs(newArrayList(
                new MetadataEntityDef(),
                new VersionEntityDef(),
                new FieldEntityDef(),
                new SerdesEntityDef(),
                new VersionStateEntityDef(),
                new BranchEntityDef()
        ));
        setRelationshipDefs(newArrayList(
                new SchemaVersionRelationshipDef(),
                new SerdesMappingRelationshipDef(),
                new SchemaFieldsRelationshipDef(),
                new VersionStateRelationshipDef(),
                new VersionBranchRelationshipDef(),
                new MetaBranchRelationshipDef()
        ));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof AtlasTypesDef)) {
            return false;
        }

        AtlasTypesDef typesDef = (AtlasTypesDef)o;

        return entityDefsMatch(typesDef) &&
                relationshipDefsMatch(typesDef);
    }

    /** Check if the provided typedef contains all the entity defs of our model. */
    private boolean entityDefsMatch(AtlasTypesDef typesDef) {
        return compareLists(this.getEntityDefs(), typesDef.getEntityDefs());
    }

    /** Check if the provided typedef contains all the entity defs of our model. */
    private boolean relationshipDefsMatch(AtlasTypesDef typesDef) {
        return compareLists(this.getRelationshipDefs(), typesDef.getRelationshipDefs());
    }

    /** Check if the provided typedef contains all the typedefs in our model. */
    private <T extends AtlasBaseTypeDef> boolean compareLists(List<T> expected, List<T> actual) {
        if (expected.isEmpty() && actual.isEmpty()) {
            return true;
        }

        // initially this list contains all expected EntityDefs; we'll remove the actual EntityDefs;
        // the remaining items will contain the items found in this model but not in the other one
        final List<T> missingEntityDefs = newArrayList();
        missingEntityDefs.addAll(expected);

        for (T entityDef : actual) {
            Iterator<T> iter = missingEntityDefs.iterator();
            while (iter.hasNext()) {
                if (iter.next().getName().equals(entityDef.getName())) {
                    iter.remove();
                    break;
                }
            }
        }
        // not a full comparison since it's possible for this list to be empty but the other
        // model to have extra entities - but for our purposes that's good enough
        return missingEntityDefs.isEmpty();
    }
}
