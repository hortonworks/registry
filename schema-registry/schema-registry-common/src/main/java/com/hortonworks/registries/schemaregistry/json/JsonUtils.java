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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

public class JsonUtils {
    public static final String NOT_PRIMITIVE = "Object is not primitive.";

    public static ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        objectMapper.registerModule(new JodaModule());
        objectMapper.registerModule(new ParameterNamesModule());
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(new JsonOrgModule());
        objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        objectMapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
        return objectMapper;
    }
    
    public static Object generatePrimitiveJsonSchema(Object object, boolean isPrimitive) {
        Object primitiveValue = new Object();
        if (object == null
            || object instanceof Boolean
            || object instanceof Number
            || object instanceof String) {
            primitiveValue = object;
            isPrimitive = true;
        } else if (object instanceof BinaryNode) {
            primitiveValue = ((BinaryNode) object).asText();
            isPrimitive = true;
        } else if (object instanceof BooleanNode) {
            primitiveValue = ((BooleanNode) object).asBoolean();
            isPrimitive = true;
        } else if (object instanceof NullNode) {
            primitiveValue = null;
            isPrimitive = true;
        } else if (object instanceof NumericNode) {
            primitiveValue = ((NumericNode) object).numberValue();
            isPrimitive = true;
        } else if (object instanceof TextNode) {
            primitiveValue = ((TextNode) object).asText();
            isPrimitive = true;
        }
        if (isPrimitive) {
            return primitiveValue;
        } else {
            return NOT_PRIMITIVE;
        }
    }
    
        public static Object generateComplexJsonSchema(Object object, ObjectMapper objectMapper) throws JsonProcessingException {
            Object complexValue;
            if (object instanceof ArrayNode) {
                complexValue = objectMapper.treeToValue(((ArrayNode) object), JSONArray.class);
            } else if (object instanceof JsonNode) {
                complexValue = objectMapper.treeToValue(((JsonNode) object), JSONObject.class);
            } else if (object.getClass().isArray()) {
                complexValue = objectMapper.convertValue(object, JSONArray.class);
            } else {
                complexValue = objectMapper.convertValue(object, JSONObject.class);
            }
            return complexValue;
    }

    private JsonUtils() { }

}
