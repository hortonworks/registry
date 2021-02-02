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
package com.hortonworks.registries.schemaregistry.exportimport.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.exportimport.RawSchema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Confluent SR stores its data in a Kafka topic. The user needs to dump
 * the contents of that topic into a file and upload it to our service.
 * This class parses the file and outputs a schema on each invocation.
 *
 * The file is in a transaction log format. For example, if a schema is created
 * and later deleted, both facts will be logged. Because of this, if we want to
 * know the final state of each schema, we need to parse the entire file.
 *
 * Example file:
 * <code>
 * {"keytype":"SCHEMA","subject":"Kafka-key","version":1,"magic":1}        {"subject":"Kafka-key","version":1,"id":1,"schema":"\"string\"","deleted":false}
 * {"keytype":"SCHEMA","subject":"Car","version":1,"magic":1}      {"subject":"Car","version":1,"id":2,"schema":"{\"type\":\"record\",\"name\":\"Car\",\"namespace\":\"com.piripocs\",\"fields\":[{\"name\":\"model\",\"type\":\"string\"},{\"name\":\"color\",\"type\":\"string\",\"default\":\"blue\"},{\"name\":\"price\",\"type\":\"string\",\"default\":\"0\"},{\"name\":\"year\",\"type\":[\"null\",\"string\"],\"default\":null}]}","deleted":false}
 * {"keytype":"SCHEMA","subject":"Car","version":2,"magic":1}      {"subject":"Car","version":2,"id":3,"schema":"{\"type\":\"record\",\"name\":\"Car\",\"namespace\":\"com.piripocs\",\"fields\":[{\"name\":\"model\",\"type\":\"string\"},{\"name\":\"color\",\"type\":\"string\",\"default\":\"blue\"},{\"name\":\"price\",\"type\":\"string\",\"default\":\"0\"}]}","deleted":false}
 * {"keytype":"CONFIG","subject":"Car","magic":0}  {"compatibilityLevel":"FULL"}
 * {"keytype":"SCHEMA","subject":"Fruit","version":1,"magic":1}    {"subject":"Fruit","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"Fruit\",\"namespace\":\"com.piripocs\",\"fields\":[{\"name\":\"color\",\"type\":\"string\",\"default\":\"green\"}]}","deleted":false}
 * {"keytype":"SCHEMA","subject":"Kafka-key","version":1,"magic":1}        {"subject":"Kafka-key","version":1,"id":1,"schema":"\"string\"","deleted":true}
 * </code>
 */
public class ConfluentFileReader implements UploadedFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentFileReader.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Supplier<Iterator<Map.Entry<Integer, RawSchema>>> parsedRows;

    public ConfluentFileReader(InputStream file) {
        parsedRows = Suppliers.memoize(() -> {
            try {
                return buildMap(file).entrySet().iterator();
            } catch (Exception iex) {
                throw new RuntimeException(iex);
            }
        });
    }

    /**
     * Parse the entire file and store the results in a map.
     * @param file              input file
     * @return      a map containing id -> schema row, sorted by id
     */
    private Map<Integer, RawSchema> buildMap(InputStream file) throws IOException {
        Map<Integer, RawSchema.Builder> result = new HashMap<>();
        Map<String, String> compatibilityLevels = new HashMap<>();  // schemas have their own compatibility config
        String globalCompatibility = "BACKWARD";  // default compatibility in Confluent SR

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file))) {
            ObjectReader json = objectMapper.reader();
            String line;
            while ((line = reader.readLine()) != null) {
                // process line; in case the line changes the global compatibility then
                // the method will return the new setting; parsed schemas are added
                // to the result map; deleted schemas are deleted from the map
                globalCompatibility = processOneLine(result, compatibilityLevels, globalCompatibility, json, line);
            }
        }

        // set the correct compatibility config for each schema
        setCompatibilityConfig(result, compatibilityLevels, globalCompatibility);

        return result.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build(), (o1, o2) -> o1, TreeMap::new));
    }

    private String processOneLine(Map<Integer, RawSchema.Builder> result, Map<String, String> compatibilityLevels,
                                  String globalCompatibility, ObjectReader json, String line) {
        try {
            if (!lineIsValid(line)) {
                LOG.debug("Skipping invalid line: {}", line);
                return globalCompatibility;
            }

            int splitAt = getPositionOfClosingBracket(line);
            if (splitAt <= 0) {
                LOG.debug("Skipping invalid line: {}", line);
                return globalCompatibility;
            }

            String key = line.substring(0, splitAt + 1);
            String value = line.substring(splitAt + 1);

            final JsonNode jsonKeyNode = json.readTree(key);
            final String subject = jsonKeyNode.get("subject").asText();
            if (StringUtils.isBlank(subject)) {
                return globalCompatibility;
            }
            final String keytype = jsonKeyNode.get("keytype").asText();
            final JsonNode jsonValueNode = json.readTree(value);

            if ("SCHEMA".equals(keytype)) {
                processSchemaNode(result, subject, jsonValueNode);
            } else if ("DELETE_SUBJECT".equals(keytype)) {
                deleteSubject(result, subject);
            } else if ("CONFIG".equals(keytype)) {
                String compat = jsonValueNode.get("compatibilityLevel").asText();
                if (StringUtils.isBlank(compat)) {
                    return globalCompatibility;
                }
                if (StringUtils.isBlank(subject)) {
                    globalCompatibility = compat;
                } else {
                    compatibilityLevels.put(subject, compat);
                }
            }
            return globalCompatibility;
        } catch (Exception ex) {
            LOG.error("Error while processing line: {}", line, ex);
            return globalCompatibility;
        }
    }

    /** Delete all schemas with the given name. */
    private void deleteSubject(Map<Integer, RawSchema.Builder> result, String subject) {
        // {"keytype":"DELETE_SUBJECT","subject":"Fruit","magic":0}
        for (Iterator<Map.Entry<Integer, RawSchema.Builder>> iterator = result.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<Integer, RawSchema.Builder> entry = iterator.next();
            SchemaMetadata metadata = entry.getValue().getMetadata();
            if (metadata != null && subject.equals(metadata.getName())) {
                iterator.remove();
            }
        }
    }

    /** Parse a single schema node and add the result to the map. */
    private void processSchemaNode(Map<Integer, RawSchema.Builder> result, String subject, JsonNode jsonValueNode) {
        // {"subject":"Kafka-key","version":1,"id":1,"schema":"\"string\"","deleted":false}
        int version = jsonValueNode.get("version").asInt(1);
        int schemaId = jsonValueNode.get("id").asInt();
        String schemaText = jsonValueNode.get("schema").asText();
        boolean deleted = jsonValueNode.get("deleted").asBoolean(false);

        if (deleted) {
            result.remove(schemaId);
        } else {
            RawSchema.Builder rawSchema;
            if (result.containsKey(schemaId)) {
                rawSchema = result.get(schemaId);
            } else {
                rawSchema = RawSchema.builder(subject);
                result.put(schemaId, rawSchema);
            }

            rawSchema.version(version).schemaText(schemaText);
        }
    }

    private void setCompatibilityConfig(Map<Integer, RawSchema.Builder> result, Map<String, String> compatibilityLevels,
                                        String globalCompatibility) {

        for (RawSchema.Builder rawSchema : result.values()) {
            SchemaMetadata metadata = rawSchema.getMetadata();
            if (metadata == null) {
                continue;
            }
            String schemaName = metadata.getName();

            String compatibility = globalCompatibility;
            if (compatibilityLevels.containsKey(schemaName)) {
                compatibility = compatibilityLevels.get(schemaName);
            }

            if (StringUtils.isNotBlank(compatibility)) {
                rawSchema.compatibility(parseConfluentCompatibility(compatibility));
                rawSchema.validationLevel(parseConfluentValidation(compatibility));
            }
        }
    }

    private SchemaCompatibility parseConfluentCompatibility(String compatibility) {
        switch (compatibility) {
            case "BACKWARD":
            case "BACKWARD_TRANSITIVE":
                return SchemaCompatibility.BACKWARD;
            case "FORWARD":
            case "FORWARD_TRANSITIVE":
                return SchemaCompatibility.FORWARD;
            case "FULL":
            case "FULL_TRANSITIVE":
                return SchemaCompatibility.BOTH;
            case "NONE": return SchemaCompatibility.NONE;
            default:
                return SchemaCompatibility.DEFAULT_COMPATIBILITY;
        }
    }

    private SchemaValidationLevel parseConfluentValidation(String compatibility) {
        switch (compatibility) {
            case "BACKWARD":
            case "FORWARD":
            case "FULL":
            case "NONE":
                return SchemaValidationLevel.LATEST;
            case "BACKWARD_TRANSITIVE":
            case "FORWARD_TRANSITIVE":
            case "FULL_TRANSITIVE":
                return SchemaValidationLevel.ALL;
            default:
                return SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL;
        }
    }

    // very simple validation
    private boolean lineIsValid(String line) {
        if (StringUtils.isBlank(line)) {
            return false;
        }
        // each line needs to contain 2 pairs of { }
        int i1 = line.indexOf('{');
        if (i1 < 0) {
            return false;
        }
        int i2 = line.indexOf('{', i1);
        if (i2 < 0) {
            return false;
        }
        int i3 = line.indexOf('}', i1);
        if (i3 < 0) {
            return false;
        }
        int i4 = line.indexOf('}', i3);
        if (i4 < 0) {
            return false;
        }
        return true;
    }

    /**
     * A line contains key and value in format {aaa}   {bbb}.
     * We need to find the position of the first closing bracket
     * in order to know where the key ends.
     *
     * @param line      input string
     * @return  position of the first block's closing bracket or -1 if not found
     */
    @VisibleForTesting
    int getPositionOfClosingBracket(String line) {
        int index = 0;
        // find position of first opening bracket
        while (index < line.length() && line.charAt(index) != '{') {
            index++;
        }
        if (index >= line.length()) {
            return -1;
        }

        int openBrackets = 1;
        index++;

        for (; index < line.length(); index++) {
            switch (line.charAt(index)) {
                case '{':
                    openBrackets++;
                    break;
                case '}':
                    openBrackets--;
                    break;
                default:
                    break;
            }

            if (openBrackets == 0) {
                return index;
            }
        }

        return -1;
    }

    @Override
    public RawSchema readSchema() {
        if (parsedRows.get().hasNext()) {
            return parsedRows.get().next().getValue();
        }

        return null;
    }
}
