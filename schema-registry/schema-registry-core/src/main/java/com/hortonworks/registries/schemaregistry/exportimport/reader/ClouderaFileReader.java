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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Parse a file that has previously been created with REST API endpoint
 * /api/v1/schemaregistry/schemas/aggregated
 *
 * Example file:
 * <code>
 *   {
 *   "entities": [
 *     {
 *       "schemaMetadata": {
 *         "type": "avro",
 *         "schemaGroup": "Kafka",
 *         "name": "Geza",
 *         "description": "geza",
 *         "compatibility": "BACKWARD",
 *         "validationLevel": "ALL",
 *         "evolve": true
 *       },
 *       "id": 3,
 *       "timestamp": 1632217883539,
 *       "schemaBranches": [
 *         {
 *           "schemaBranch": {
 *             "id": 3,
 *             "name": "MASTER",
 *             "schemaMetadataName": "Geza",
 *             "description": "'MASTER' branch for schema metadata 'Geza'",
 *             "timestamp": 1632217883540
 *           },
 *           "rootSchemaVersion": null,
 *           "schemaVersionInfos": [
 *             {
 *               "id": 4,
 *               "schemaMetadataId": 3,
 *               "name": "Geza",
 *               "description": "bool",
 *               "version": 2,
 *               "schemaText": "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Geza\",\n     \"fields\": [\n       { \"name\": \"Geza\", \"type\": \"string\" },\n       { \"name\": \"name\", \"type\": \"string\" },\n       { \"name\": \"isGeza\", \"type\": \"boolean\", \"default\": false }\n     ]\n} ",
 *               "timestamp": 1632217919782,
 *               "stateId": 5,
 *               "mergeInfo": null
 *             },
 *             {
 *               "id": 3,
 *               "schemaMetadataId": 3,
 *               "name": "Geza",
 *               "description": "geza",
 *               "version": 1,
 *               "schemaText": "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Geza\",\n     \"fields\": [\n       { \"name\": \"Geza\", \"type\": \"string\" },\n       { \"name\": \"name\", \"type\": \"string\" }\n     ]\n} ",
 *               "timestamp": 1632217883747,
 *               "stateId": 5,
 *               "mergeInfo": null
 *             }
 *           ]
 *         }
 *       ],
 *       "serDesInfos": []
 *     },
 *     {
 *       "schemaMetadata": {
 *         "type": "avro",
 *         "schemaGroup": "Kafka",
 *         "name": "Car",
 *         "description": "br",
 *         "compatibility": "BACKWARD",
 *         "validationLevel": "ALL",
 *         "evolve": true
 *       },
 *       "id": 2,
 *       "timestamp": 1632217853151,
 *       "schemaBranches": [
 *         {
 *           "schemaBranch": {
 *             "id": 2,
 *             "name": "MASTER",
 *             "schemaMetadataName": "Car",
 *             "description": "'MASTER' branch for schema metadata 'Car'",
 *             "timestamp": 1632217853152
 *           },
 *           "rootSchemaVersion": null,
 *           "schemaVersionInfos": [
 *             {
 *               "id": 2,
 *               "schemaMetadataId": 2,
 *               "name": "Car",
 *               "description": "br",
 *               "version": 1,
 *               "schemaText": "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Car\",\n     \"fields\": [\n       { \"name\": \"color\", \"type\": \"string\" },\n       { \"name\": \"year\", \"type\": \"int\" }\n     ]\n} ",
 *               "timestamp": 1632217853364,
 *               "stateId": 5,
 *               "mergeInfo": null
 *             }
 *           ]
 *         },
 *         {
 *           "schemaBranch": {
 *             "id": 4,
 *             "name": "Tractor",
 *             "schemaMetadataName": "Car",
 *             "description": "gggg",
 *             "timestamp": 1632217946367
 *           },
 *           "rootSchemaVersion": 2,
 *           "schemaVersionInfos": [
 *             {
 *               "id": 5,
 *               "schemaMetadataId": 2,
 *               "name": "Car",
 *               "description": "speed",
 *               "version": 2,
 *               "schemaText": "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Car\",\n     \"fields\": [\n       { \"name\": \"color\", \"type\": \"string\" },\n       { \"name\": \"year\", \"type\": \"int\" },\n       { \"name\": \"max_speed\", \"type\": \"double\", \"default\" : \"100.0\" }\n     ]\n} ",
 *               "timestamp": 1632218012920,
 *               "stateId": 1,
 *               "mergeInfo": null
 *             },
 *             {
 *               "id": 2,
 *               "schemaMetadataId": 2,
 *               "name": "Car",
 *               "description": "br",
 *               "version": 1,
 *               "schemaText": "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Car\",\n     \"fields\": [\n       { \"name\": \"color\", \"type\": \"string\" },\n       { \"name\": \"year\", \"type\": \"int\" }\n     ]\n} ",
 *               "timestamp": 1632217853364,
 *               "stateId": 5,
 *               "mergeInfo": null
 *             }
 *           ]
 *         }
 *       ],
 *       "serDesInfos": []
 *     },
 *     {
 *       "schemaMetadata": {
 *         "type": "avro",
 *         "schemaGroup": "Kafka",
 *         "name": "FullName",
 *         "description": "asdasdasd",
 *         "compatibility": "BACKWARD",
 *         "validationLevel": "ALL",
 *         "evolve": true
 *       },
 *       "id": 1,
 *       "timestamp": 1632217822615,
 *       "schemaBranches": [
 *         {
 *           "schemaBranch": {
 *             "id": 1,
 *             "name": "MASTER",
 *             "schemaMetadataName": "FullName",
 *             "description": "'MASTER' branch for schema metadata 'FullName'",
 *             "timestamp": 1632217822619
 *           },
 *           "rootSchemaVersion": null,
 *           "schemaVersionInfos": [
 *             {
 *               "id": 1,
 *               "schemaMetadataId": 1,
 *               "name": "FullName",
 *               "description": "asdasdasd",
 *               "version": 1,
 *               "schemaText": "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"FullName\",\n     \"fields\": [\n       { \"name\": \"first\", \"type\": \"string\" },\n       { \"name\": \"last\", \"type\": \"string\" }\n     ]\n} ",
 *               "timestamp": 1632217822893,
 *               "stateId": 5,
 *               "mergeInfo": null
 *             }
 *           ]
 *         }
 *       ],
 *       "serDesInfos": []
 *     }
 *   ]
 * }
 * </code>
 */
public class ClouderaFileReader {
    private static final Logger LOG = LoggerFactory.getLogger(ClouderaFileReader.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private InputStream file;

    @VisibleForTesting
    ClouderaFileReader() {
    }

    public ClouderaFileReader(InputStream file) {
        this.file = file;
    }

    public void setFile(InputStream file) {
        this.file = file;
    }

    public List<AggregatedSchemaMetadataInfo> getMetadataInfos() {
        ImmutableList.Builder<AggregatedSchemaMetadataInfo> metadataInfos = ImmutableList.builder();
        try {

            JsonNode jsonNode = mapper.readValue(file,
                JsonNode.class);
            checkNotNull(jsonNode, "Values in file can not be read");
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                Iterator<JsonNode> iterator = entry.getValue().iterator();
                while (iterator.hasNext()) {
                    JsonNode metadataInfo = iterator.next();
                    AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo = mapper.readValue(metadataInfo.traverse(), AggregatedSchemaMetadataInfo.class);
                    checkNotNull(aggregatedSchemaMetadataInfo, "AggregatedMetadataInfo is null");
                    metadataInfos.add(aggregatedSchemaMetadataInfo);
                }
            }
        } catch (Exception e) {
            LOG.error("Error while reading schemas", e);
        }
        return metadataInfos.build();
    }
}
