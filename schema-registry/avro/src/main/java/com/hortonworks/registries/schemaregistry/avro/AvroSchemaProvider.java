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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.SchemaParseException;
import org.apache.avro.SchemaValidationException;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class AvroSchemaProvider implements SchemaProvider {

    public static final String TYPE = "avro";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean isCompatible(String toSchemaText, String existingSchemaText, Compatibility existingSchemaCompatibility) {
        return isCompatible(toSchemaText, Collections.singleton(existingSchemaText), existingSchemaCompatibility);
    }

    public boolean isCompatible(String toSchemaText, Collection<String> existingSchemaTexts, Compatibility existingSchemaCompatibility) {
        Schema toSchema = new Schema.Parser().parse(toSchemaText);

        Collection<Schema> existingSchemas = new ArrayList<>();
        for (String schemaText : existingSchemaTexts) {
            existingSchemas.add(new Schema.Parser().parse(schemaText));
        }

        try {
            SchemaCompatibilityValidator.of(existingSchemaCompatibility).validate(toSchema, existingSchemas);
        } catch (SchemaValidationException e) {
            return false;
        }

        return true;
    }

    @Override
    public byte[] getFingerprint(String schemaText) throws InvalidSchemaException {
        try {
            // generates fingerprint of canonical form of the given schema.
            Schema schema = new Schema.Parser().parse(schemaText);
            return SchemaNormalization.parsingFingerprint("MD5", schema);
        } catch (SchemaParseException e) {
            throw new InvalidSchemaException("Given schema is invalid", e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SchemaFieldInfo> generateFields(String rootSchema) {
        AvroFieldsGenerator avroFieldsGenerator = new AvroFieldsGenerator();
        return avroFieldsGenerator.generateFields(new Schema.Parser().parse(rootSchema));
    }

}
