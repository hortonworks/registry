/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.streams;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import com.hortonworks.registries.schemaregistry.SchemaProvider;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class StreamsSchemaProvider implements SchemaProvider {
    public static final String TYPE = "streams";
    public static final String UTF_8 = "UTF-8";
    public static final String MD5 = "MD5";

    public StreamsSchemaProvider() {
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean isCompatible(String toSchema,
                                String existingSchema,
                                Compatibility compatibility) {
        return true;
    }

    @Override
    public boolean isCompatible(String toSchemaText,
                                Collection<String> existingSchemaTexts,
                                Compatibility existingSchemaCompatibility) {
        return true;
    }

    @Override
    public byte[] getFingerprint(String schemaText) throws InvalidSchemaException {
        try {
            return MessageDigest.getInstance(MD5).digest(schemaText.getBytes(UTF_8));
        } catch (Exception e) {
            throw new InvalidSchemaException(e);
        }
    }

    @Override
    public List<SchemaFieldInfo> generateFields(String rootSchema) {
        Schema schema = Schema.fromString(rootSchema);
        List<Schema.Field> fields = schema.getFields();
        List<SchemaFieldInfo> fieldInfos = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            // currently internal schema implementation does not have namespace.
            fieldInfos.add(new SchemaFieldInfo("__universal", field.getName(), field.getType().toString()));
        }

        return fieldInfos;
    }
}
