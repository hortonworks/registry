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
package com.hortonworks.registries.schemaregistry.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.serde.SerDeException;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is the default implementation of {@link ISchemaRegistryClient} which connects to the given {@code rootCatalogURL}.
 * <pre>
 * This can be used to
 *      - register schemas
 *      - add new versions of a schema
 *      - fetch different versions of schema
 *      - fetch latest version of a schema
 *      - check whether the given schema text is compatible with a latest version of the schema
 *      - register serializer/deserializer for a schema
 *      - fetch serializer/deserializer for a schema
 * </pre>
 */
public class SchemaRegistryClient implements ISchemaRegistryClient {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(SchemaRegistryClient.class);

    public static final String SCHEMA_REGISTRY_PATH = "/schemaregistry";
    private static final String SCHEMAS_PATH = SCHEMA_REGISTRY_PATH + "/schemas/";
    private static final String FILES_PATH = SCHEMA_REGISTRY_PATH + "/files/";
    private static final String SERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/serializers/";
    private static final String DESERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/deserializers/";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String LOCAL_JAR_PATH = "schema.registry.local.jars.path";
    public static final String CLASSLOADER_CACHE_SIZE = "schema.registry.class.loader.cache.size";

    private Client client;
    private WebTarget rootTarget;
    private WebTarget schemasTarget;

    public SchemaRegistryClient() {
    }

    @Override
    public void init(Map<String, Object> conf) {
        client = ClientBuilder.newClient(new ClientConfig());
        client.register(MultiPartFeature.class);
        String rootCatalogURL = (String) conf.get(SCHEMA_REGISTRY_URL);
        rootTarget = client.target(rootCatalogURL);
        schemasTarget = rootTarget.path(SCHEMAS_PATH);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public SchemaKey registerSchema(SchemaMetadata schemaMetadata) {
        return postEntity(schemasTarget, schemaMetadata, SchemaKey.class);
    }

    @Override
    public SchemaKey addVersionedSchema(Long schemaMetadataId, VersionedSchema schemaInfo) throws InvalidSchemaException, IncompatibleSchemaException {
        WebTarget path = schemasTarget.path(schemaMetadataId.toString());
        return postEntity(path, schemaInfo, SchemaKey.class);
    }

    private <T> List<T> getEntities(WebTarget target, Class<T> clazz) {
        List<T> entities = new ArrayList<>();
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            Iterator<JsonNode> it = node.get("entities").elements();
            while (it.hasNext()) {
                entities.add(mapper.treeToValue(it.next(), clazz));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return entities;
    }

    private <T> T postEntity(WebTarget target, Object json, Class<T> responseType) {
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(json), String.class);

        return readEntity(response, responseType);
    }

    private <T> T readEntity(String response, Class<T> clazz) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            return mapper.treeToValue(node.get("entity"), clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private <T> T getEntity(WebTarget target, Class<T> clazz) {
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);

        return readEntity(response, clazz);
    }

    @Override
    public Iterable<SchemaInfo> listAllSchemas() {
        return getEntities(schemasTarget, SchemaInfo.class);
    }

    @Override
    public SchemaInfo getSchema(SchemaKey schemaKey) {
        return getEntity(schemasTarget.path(String.format("%d/versions/%d", schemaKey.getId(), schemaKey.getVersion())), SchemaInfo.class);
    }

    @Override
    public SchemaInfo getLatestSchema(Long schemaMetadataId) {
        return getEntity(schemasTarget.path(String.format("%d/versions/latest", schemaMetadataId)), SchemaInfo.class);
    }

    @Override
    public Iterable<SchemaInfo> getAllVersions(Long schemaMetadataId) {
        return getEntities(schemasTarget.path(schemaMetadataId.toString()), SchemaInfo.class);
    }

    @Override
    public boolean isCompatibleWithLatestSchema(Long schemaMetadataId, String toSchemaText) {
        WebTarget target = schemasTarget.path(String.format("compatibility/%d/versions/latest", schemaMetadataId));
        String response = target.request().post(Entity.text(toSchemaText), String.class);
        return readEntity(response, Boolean.class);
    }

    @Override
    public String uploadFile(InputStream inputStream) {
        MultiPart multiPart = new MultiPart();
        BodyPart filePart = new StreamDataBodyPart("file", inputStream, "file");
        multiPart.bodyPart(filePart);

        String response = rootTarget.path(FILES_PATH).request().post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA), String.class);
        return readEntity(response, String.class);
    }

    @Override
    public InputStream downloadFile(String fileId) {
        return rootTarget.path(FILES_PATH).path("download/"+fileId).request().get(InputStream.class);
    }

    @Override
    public Long addSerializer(SerDesInfo serializerInfo) {
        return postEntity(rootTarget.path(SERIALIZERS_PATH), serializerInfo, Long.class);
    }

    @Override
    public Long addDeserializer(SerDesInfo deserializerInfo) {
        String response = postEntity(rootTarget.path("deserializers"), deserializerInfo, String.class);
        return readEntity(response, Long.class);
    }

    public void mapSchemaWithSerDes(Long schemaMetadataId, Long serDesId) {
        Boolean success = postEntity(schemasTarget.path("mapping/" + schemaMetadataId + "/" + serDesId), null, Boolean.class);
        LOG.info("Received response while mapping schema [{}] with serialzer/deserializer [{}] : [{}]", schemaMetadataId, serDesId, success);
    }

    @Override
    public Collection<SerDesInfo> getSerializers(Long schemaMetadataId) {
        return getEntities(schemasTarget.path(schemaMetadataId+"/serializers/"), SerDesInfo.class);
    }

    @Override
    public Collection<SerDesInfo> getDeserializers(Long schemaMetadataId) {
        return getEntities(schemasTarget.path(schemaMetadataId+"/deserializers/"), SerDesInfo.class);
    }

    public <T> T createSerializerInstance(SerDesInfo serializerInfo) {
        return createInstance(serializerInfo);
    }

    @Override
    public <T> T createDeserializerInstance(SerDesInfo deserializerInfo) {
        return createDeserializerInstance(deserializerInfo);
    }

    private <T> T createInstance(SerDesInfo serializerInfo) {
        // loading serializer, create a class loader and and keep them in cache.
        String fileId = serializerInfo.getFileId();
        // get class loader for this file ID
        ClassLoader classLoader = getClassLoader(fileId);

        //
        T t = null;
        try {
            Class<T> clazz = (Class<T>) Class.forName(serializerInfo.getClassName(), true, classLoader);
            t = clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new SerDeException(e);
        }

        return t;
    }

    private ClassLoader getClassLoader(String fileId) {
        //todo cache of files with respective classloaders
        return null;
    }

}
