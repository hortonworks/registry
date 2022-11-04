/**
 * Copyright 2016-2022 Cloudera, Inc.
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
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaResolver;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.everit.json.schema.loader.SpecificationVersion;
import org.everit.json.schema.loader.internal.ReferenceResolver;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class JsonSchemaResolver implements SchemaResolver {

    private static final String SCHEMA_KEYWORD = "$schema";

    private final ObjectMapper objectMapper = JsonUtils.getObjectMapper();
    private final Map<String, String> resolvedReferences = new HashMap<>();

    private final SchemaVersionRetriever schemaVersionRetriever;

    public JsonSchemaResolver(SchemaVersionRetriever schemaVersionRetriever) {
        this.schemaVersionRetriever = schemaVersionRetriever;
    }

    @Override
    public String resolveSchema(String schemaText) throws InvalidSchemaException {
        return parseSchema(schemaText).toString();
    }

    @Override
    public String resolveSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
        String schemaText = schemaVersionRetriever.retrieveSchemaVersion(schemaVersionKey).getSchemaText();
        return parseSchema(schemaText).toString();
    }

    @Override
    public String resolveSchema(SchemaIdVersion schemaIdVersion) throws InvalidSchemaException, SchemaNotFoundException {
        String schemaText = schemaVersionRetriever.retrieveSchemaVersion(schemaIdVersion).getSchemaText();
        return parseSchema(schemaText).toString();
    }

    private Schema parseSchema(String schemaText) throws InvalidSchemaException {
        try {
            JsonNode jsonNode = objectMapper.readTree(schemaText);

            // Extract the $schema to use for determining the id keyword
            SpecificationVersion spec = SpecificationVersion.DRAFT_7;
            if (jsonNode.has(SCHEMA_KEYWORD)) {
                String schema = jsonNode.get(SCHEMA_KEYWORD).asText();
                if (schema != null) {
                    spec = SpecificationVersion.lookupByMetaSchemaUrl(schema).orElse(SpecificationVersion.DRAFT_7);
                }
            }

            // Extract the $id to use for resolving relative $ref URIs
            URI idUri = null;
            if (jsonNode.has(spec.idKeyword())) {
                String id = jsonNode.get(spec.idKeyword()).asText();
                if (id != null) {
                    idUri = ReferenceResolver.resolve((URI) null, id);
                }
            }

            SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder().useDefaults(true).draftV7Support();

            for (Map.Entry<String, String> dep : resolvedReferences.entrySet()) {
                URI child = ReferenceResolver.resolve(idUri, dep.getKey());
                builder.registerSchemaByURI(child, new JSONObject(dep.getValue()));
            }

            JSONObject jsonObject = objectMapper.treeToValue(jsonNode, JSONObject.class);
            builder.schemaJson(jsonObject);
            SchemaLoader loader = builder.build();
            return loader.load().build();
        } catch (IOException iex) {
            throw new InvalidSchemaException("Invalid JSON", iex);
        }
    }

}
