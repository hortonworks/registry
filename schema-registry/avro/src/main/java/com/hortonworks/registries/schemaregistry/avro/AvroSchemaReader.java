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

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class AvroSchemaReader {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaReader.class);

    public AvroSchemaReader() {
    }

    public List<FieldInfo> parse(Schema rootSchema) {
        List<FieldInfo> fieldInfos = new ArrayList<>();
        parse(rootSchema, fieldInfos);

        return fieldInfos;
    }

    public void parse(Schema schema, List<FieldInfo> fieldInfos) {
        String fullName = schema.getFullName();
        String namespace = schema.getNamespace();
        Set<String> aliases = schema.getAliases();

        LOG.debug("Full name: [{}], namespace: [{}], aliases: [{}]", fullName, namespace, aliases);

        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
            parseField(field, fieldInfos);
        }

    }

    private void parseField(Schema.Field field, List<FieldInfo> fieldInfos) {
        Schema schema = field.schema();
        Schema.Type type = schema.getType();
        String name = field.name();

        Set<String> aliases = field.aliases();
        JsonNode jsonNode = field.defaultValue();
        LOG.debug("Visiting field: [{}]", field);
        String namespace = null;
        try {
            namespace = schema.getNamespace();
        } catch (Exception e) {
            //ignore.
        }
        fieldInfos.add(new FieldInfo(namespace, name, type.name()));

        // todo check whether fields should be mapped to the root schema.
        // should be mapped to the parent element.
        handleSchema(schema, fieldInfos);
    }

    private void handleSchema(Schema schema, List<FieldInfo> fieldInfos) {
        Schema.Type type = schema.getType();
        LOG.debug("Visiting type: [{}]", type);

        switch (type) {
            case RECORD:
                // store fields of a record.
                List<Schema.Field> fields = schema.getFields();
                for (Schema.Field recordField : fields) {
                    parseField(recordField, fieldInfos);
                }
                break;
            case MAP:
                Schema valueTypeSchema = schema.getValueType();
                handleSchema(valueTypeSchema, fieldInfos);
                break;
            case ENUM:
                break;
            case ARRAY:
                Schema elementType = schema.getElementType();
                handleSchema(elementType, fieldInfos);
                break;

            case UNION:
                List<Schema> unionTypes = schema.getTypes();
                for (Schema typeSchema : unionTypes) {
                    handleSchema(typeSchema, fieldInfos);
                }
                break;

            case STRING:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case FIXED:
            case BOOLEAN:
            case BYTES:
            case NULL:

                break;

            default:
                throw new RuntimeException("Unsupported type: " + type);

        }

    }

    private String getFullName(Schema.Field field) {
        return field.schema().getNamespace() + "." + field.name();
    }

    static class FieldInfo implements Serializable {
        public final String namespace;
        public final String name;
        public final String type;

        public FieldInfo(String namespace, String name, String type) {
            this.namespace = namespace;
            this.name = name;
            this.type = type;
        }
    }
}
