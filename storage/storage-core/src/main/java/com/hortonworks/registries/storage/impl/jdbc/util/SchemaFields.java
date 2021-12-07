/*
  Copyright 2016-2021 Cloudera, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package com.hortonworks.registries.storage.impl.jdbc.util;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.Storable;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.hortonworks.registries.common.Schema.Type.INTEGER;
import static com.hortonworks.registries.common.Schema.Type.LONG;
import static java.util.stream.Collectors.toSet;

public class SchemaFields {
    private SchemaFields() {
    }

    private static boolean isNumeric(Schema.Field field) {
        return field.getType() == INTEGER || field.getType() == LONG;
    }

    public static Set<Schema.Field> idFieldsFor(Storable storable) {
        return storable.getSchema().getFields().stream()
                .filter(field -> field.getName().equalsIgnoreCase("id"))
                .filter(SchemaFields::isNumeric).collect(toSet());
    }

    public static Optional<Schema.Field> getSequenceField(PrimaryKey primaryKey) {
        Set<Schema.Field> numericPkFields = primaryKey.getFieldsToVal().keySet().stream()
                .filter(SchemaFields::isNumeric)
                .collect(toSet());

        return numericPkFields.size() == 1 ? Optional.of(getOnlyElement(numericPkFields)) : Optional.empty();
    }

    public static boolean needsSequence(Storable storable) {
        if (storable.isIdAutoIncremented()) {
            return false;
        } else {
            return !idFieldsFor(storable).isEmpty() || getSequenceField(storable.getPrimaryKey()).isPresent();
        }
    }
}
