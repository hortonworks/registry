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
package com.hortonworks.iotas.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import javax.ws.rs.core.MediaType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class AbstractRestIntegrationTest {

    protected MultiPart getMultiPart(ResourceTestElement resourceToTest, Object entity) {
        MultiPart multiPart = new MultiPart();
        BodyPart filePart = new FileDataBodyPart(resourceToTest.getFileNameHeader(), resourceToTest.getFileToUpload());
        BodyPart entityPart = new FormDataBodyPart(resourceToTest.getEntityNameHeader(), entity, MediaType.APPLICATION_JSON_TYPE);
        multiPart.bodyPart(filePart).bodyPart(entityPart);
        return multiPart;
    }

    public <T> T filterFields(T object, List<String> fields) throws Exception {
        if (fields != null && !fields.isEmpty()) {
            Class<?> clazz = object.getClass();
            for (String fieldName : fields) {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(object, null);
            }
        }
        return object;
    }

    /**
     * Get response code from the response string.
     *
     * @param response
     * @return
     * @throws Exception
     */
    public int getResponseCode(String response) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(response);
        return mapper.treeToValue(node.get("responseCode"), Integer.class);
    }

    protected <T extends Object> List<T> getEntities(String response, Class<T> clazz) {
        return getEntities(response, clazz, Collections.<String>emptyList());
    }

    /**
     * Get the entities from response string
     *
     * @param response
     * @param clazz
     * @param <T>
     * @return
     */
    protected <T extends Object> List<T> getEntities(String response, Class<T> clazz,
                                                     List<String> fieldsToIgnore) {
        List<T> entities = new ArrayList<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            Iterator<JsonNode> it = node.get("entities").elements();
            while (it.hasNext()) {
                entities.add(filterFields(mapper.treeToValue(it.next(), clazz), fieldsToIgnore));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return entities;
    }

    protected <T> T getEntity(String response, Class<T> clazz) {
        return getEntity(response, clazz, Collections.<String>emptyList());
    }

    /**
     * Get entity from the response string.
     *
     * @param response
     * @param clazz
     * @param <T>
     * @return
     */
    protected <T> T getEntity(String response, Class<T> clazz,
                              List<String> fieldsToIgnore) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            return filterFields(mapper.treeToValue(node.get("entity"), clazz), fieldsToIgnore);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
