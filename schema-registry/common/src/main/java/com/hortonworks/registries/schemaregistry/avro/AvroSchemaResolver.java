/*
 * Copyright 2016 Hortonworks.
 *
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.SchemaResolver;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.errors.CyclicSchemaDependencyException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AvroSchemaResolver implements SchemaResolver {

    private enum SchemaParsingState {
        PARSING, PARSED
    }

    private final SchemaVersionRetriever schemaVersionRetriever;

    public AvroSchemaResolver(SchemaVersionRetriever schemaVersionRetriever) {
        this.schemaVersionRetriever = schemaVersionRetriever;
    }

    @Override
    public String resolveSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
        Map<String, SchemaParsingState> schemaParsingStates = new HashMap<>();
        schemaParsingStates.put(schemaVersionKey.getSchemaName(), SchemaParsingState.PARSING);
        return getResultantSchema(schemaVersionKey, schemaParsingStates);
    }

    public String resolveSchema(String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        Map<String, SchemaParsingState> schemaParsingStates = new HashMap<>();
        return getResultantSchema(schemaText, schemaParsingStates);
    }

    private String getResultantSchema(SchemaVersionKey schemaVersionKey,
                                      Map<String, SchemaParsingState> schemaParsingStates)
            throws InvalidSchemaException, SchemaNotFoundException {
        String schemaText = schemaVersionRetriever.retrieveSchemaVersion(schemaVersionKey).getSchemaText();
        return getResultantSchema(schemaText, schemaParsingStates);
    }

    private String getResultantSchema(String schemaText, Map<String, SchemaParsingState> schemaParsingStates)
            throws InvalidSchemaException, SchemaNotFoundException {
        Map<String, Schema> schemaTypes = getIncludedSchemaTypes(schemaText, schemaParsingStates);
        if (schemaTypes == null || schemaTypes.isEmpty()) {
            return schemaText;
        }

        Schema.Parser parser = new Schema.Parser();
        parser.addTypes(schemaTypes);
        Schema schema = parser.parse(schemaText);

        return schema.toString();
    }

    private Map<String, Schema> getIncludedSchemaTypes(String schemaText,
                                                       Map<String, SchemaParsingState> schemaParsingStates)
            throws InvalidSchemaException, SchemaNotFoundException {
        List<SchemaVersionKey> includedSchemaVersions = getIncludedSchemaVersions(schemaText);

        if (includedSchemaVersions == null || includedSchemaVersions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Schema> schemaTypes = new HashMap<>();
        for (SchemaVersionKey schemaVersionKey : includedSchemaVersions) {
            Map<String, Schema> collectedSchemas = collectSchemaTypes(schemaVersionKey, schemaParsingStates);
            if (collectedSchemas != null) {
                schemaTypes.putAll(collectedSchemas);
            }
        }
        return schemaTypes;
    }

    private Map<String, Schema> collectSchemaTypes(SchemaVersionKey schemaVersionKey,
                                                   Map<String, SchemaParsingState> schemaParsingStates)
            throws SchemaNotFoundException, InvalidSchemaException {

        String schemaName = schemaVersionKey.getSchemaName();
        SchemaParsingState schemaParsingState = schemaParsingStates.putIfAbsent(schemaName, SchemaParsingState.PARSING);

        // if it is already parsed then the respective schema types would have been already collected.
        if (SchemaParsingState.PARSED == schemaParsingState) {
            return null;
        }

        // if it is in parsing state earlier and it is visted again then ther eis circular dependency!!
        if (SchemaParsingState.PARSING == schemaParsingState) {
            throw new CyclicSchemaDependencyException("Cyclic dependency of schema imports with schema [" + schemaName + "]");
        }

        // this schema is not yet parsed till now
        if (schemaParsingState == null) {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(getResultantSchema(schemaVersionKey, schemaParsingStates));
            Map<String, Schema> complexTypes = new HashMap<>();
            collectComplexTypes(schema, complexTypes);
            schemaParsingStates.put(schemaName, SchemaParsingState.PARSED);
            return complexTypes;
        }

        throw new IllegalStateException("Schema parsing with schema version " + schemaVersionKey + " is in invalid state!!");
    }

    private void collectComplexTypes(Schema schema, Map<String, Schema> complexTypes) {
        switch (schema.getType()) {
            case RECORD:
                complexTypes.put(schema.getFullName(), schema);
                List<Schema.Field> fields = schema.getFields();
                for (Schema.Field field : fields) {
                    collectComplexTypes(field.schema(), complexTypes);
                }
                break;
            case ARRAY:
                complexTypes.put(schema.getFullName(), schema);
                collectComplexTypes(schema.getElementType(), complexTypes);
                break;
            case UNION:
                complexTypes.put(schema.getFullName(), schema);
                List<Schema> unionSchemas = schema.getTypes();
                for (Schema schemaEntry : unionSchemas) {
                    collectComplexTypes(schemaEntry, complexTypes);
                }
                break;
            case MAP:
                complexTypes.put(schema.getFullName(), schema);
                collectComplexTypes(schema.getValueType(), complexTypes);
                break;
            default:
        }
    }

    private List<SchemaVersionKey> getIncludedSchemaVersions(String schemaText) throws InvalidSchemaException {
        JsonNode jsonNode = null;
        try {
            jsonNode = new ObjectMapper().readTree(schemaText);
        } catch (IOException e) {
            throw new InvalidSchemaException(e);
        }
        JsonNode includeSchemaNodes = jsonNode.get("includeSchemas");
        List<SchemaVersionKey> includedSchemaVersions = new ArrayList<>();
        if (includeSchemaNodes != null) {
            if (!includeSchemaNodes.isArray()) {
                throw new InvalidSchemaException("includeSchemas should be an array of strings");
            }

            for (JsonNode includeSchema : includeSchemaNodes) {
                String name = includeSchema.get("name").asText();
                JsonNode versionNode = includeSchema.get("version");
                int version = versionNode != null ? versionNode.asInt() : SchemaVersionKey.LATEST_VERSION;
                includedSchemaVersions.add(new SchemaVersionKey(name, version));
            }
        }
        return includedSchemaVersions;
    }
}
