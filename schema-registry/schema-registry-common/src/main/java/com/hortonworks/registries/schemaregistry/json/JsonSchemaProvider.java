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
package com.hortonworks.registries.schemaregistry.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.hortonworks.registries.schemaregistry.AbstractSchemaProvider;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;

public class JsonSchemaProvider extends AbstractSchemaProvider {

    public static final String TYPE = "json";

    private final ObjectMapper objectMapper = JsonUtils.getObjectMapper();

    @Override
    public String getName() {
        return "JSON schema provider";
    }

    @Override
    public String getDescription() {
        return "This provider supports JSON schemas. You can find more information about JSON schemas at https://json-schema.org/";
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public CompatibilityResult checkCompatibility(String targetSchema, String srcSchema, SchemaCompatibility compatibility) {
        return CompatibilityResult.SUCCESS;
    }

    @Override
    public byte[] getFingerprint(String schemaText) throws InvalidSchemaException {
        try {
            return MessageDigest.getInstance(getHashFunction()).digest(getResultantSchema(schemaText).getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SchemaFieldInfo> generateFields(String schemaText) throws InvalidSchemaException {
        try {
            JsonNode jsonNode = objectMapper.readTree(schemaText);
            JsonNode idNode = jsonNode.get("$id");
            JsonNode properties = jsonNode.get("properties");

            ImmutableList.Builder<SchemaFieldInfo> result = ImmutableList.builder();

            String namespace = idNode == null ? "" : idNode.asText("");
            if (properties != null) {
                for (Iterator<String> iter = properties.fieldNames(); iter.hasNext(); ) {
                    String propertyName = iter.next();
                    JsonNode property = properties.get(propertyName);
                    JsonNode propertyType = property.get("type");
                    String type = propertyType == null ? "string" : propertyType.asText("string");

                    SchemaFieldInfo fieldInfo = new SchemaFieldInfo(namespace, propertyName, type);
                    result.add(fieldInfo);
                }
            }

            return result.build();
        } catch (IOException iex) {
            throw new InvalidSchemaException("Invalid JSON", iex);
        }
    }

    @Override
    public String getResultantSchema(String schemaText) throws InvalidSchemaException {
        JsonSchemaResolver jsonSchemaResolver = new JsonSchemaResolver(getSchemaVersionRetriever());
        return jsonSchemaResolver.resolveSchema(schemaText);
    }


}
