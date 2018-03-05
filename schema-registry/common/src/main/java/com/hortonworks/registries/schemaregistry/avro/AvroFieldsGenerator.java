/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class AvroFieldsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AvroFieldsGenerator.class);

    public AvroFieldsGenerator() {
    }

    public List<SchemaFieldInfo> generateFields(Schema rootSchema) {
        List<SchemaFieldInfo> schemaFieldInfos = new ArrayList<>();
        parse(rootSchema, schemaFieldInfos);
        return schemaFieldInfos;
    }

    private void parse(Schema schema, List<SchemaFieldInfo> schemaFieldInfos) {
        if (schema.getType() != Schema.Type.RECORD) {
            LOG.info("Given schema type [{}] is not record", schema.getType());
        } else {
            String fullName = schema.getFullName();
            LOG.debug("Schema full name: [{}]", fullName);

            List<Schema.Field> fields = schema.getFields();
            Set<String> visitedRecords = new HashSet<>();
            visitedRecords.add(schema.getFullName());
            for (Schema.Field field : fields) {
                parseField(field, schemaFieldInfos, visitedRecords);
            }
        }
    }

    private void parseField(Schema.Field field, List<SchemaFieldInfo> schemaFieldInfos, Set<String> visitedRecords) {
        Schema schema = field.schema();
        Schema.Type type = schema.getType();
        String name = field.name();

        LOG.debug("Visiting field: [{}]", field);
        String namespace = null;
        try {
            namespace = schema.getNamespace();
        } catch (Exception e) {
            //ignore.
        }
        schemaFieldInfos.add(new SchemaFieldInfo(namespace, name, type.name()));

        // todo check whether fields should be mapped to the root schema.
        parseSchema(schema, schemaFieldInfos, visitedRecords);
    }

    private void parseSchema(Schema schema, List<SchemaFieldInfo> schemaFieldInfos, Set<String> visitedRecords) {
        Schema.Type type = schema.getType();
        LOG.debug("Visiting type: [{}]", type);

        switch (type) {
            case RECORD:

                String completeName = schema.getFullName();

                // Since we are only interested in primitive data types, if we encounter a record that was already parsed it can be ignored
                if (!visitedRecords.contains(completeName)) {
                    visitedRecords.add(completeName);

                    // store fields of a record.
                    List<Schema.Field> fields = schema.getFields();
                    for (Schema.Field recordField : fields) {
                        parseField(recordField, schemaFieldInfos, visitedRecords);
                    }
                }
                break;
            case MAP:
                Schema valueTypeSchema = schema.getValueType();
                parseSchema(valueTypeSchema, schemaFieldInfos, visitedRecords);
                break;
            case ENUM:
                break;
            case ARRAY:
                Schema elementType = schema.getElementType();
                parseSchema(elementType, schemaFieldInfos, visitedRecords);
                break;

            case UNION:
                List<Schema> unionTypes = schema.getTypes();
                for (Schema typeSchema : unionTypes) {
                    parseSchema(typeSchema, schemaFieldInfos, visitedRecords);
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

}
