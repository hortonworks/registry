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
import com.google.common.collect.Sets;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.util.ClassLoaderAwareInvocationHandler;
import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullDeserializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullSerializer;
import com.hortonworks.registries.schemaregistry.serde.push.PushDeserializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.*;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Options.*;

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
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryClient.class);

    private static final String SCHEMA_REGISTRY_PATH = "/schemaregistry";
    private static final String SCHEMAS_PATH = SCHEMA_REGISTRY_PATH + "/schemas/";
    private static final String FILES_PATH = SCHEMA_REGISTRY_PATH + "/files/";
    private static final String SERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/serializers/";
    private static final String DESERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/deserializers/";
    private static final Set<Class<?>> DESERIALIZER_INTERFACE_CLASSES = Sets.newHashSet(SnapshotDeserializer.class, PullDeserializer.class, PushDeserializer.class);
    private static final Set<Class<?>> SERIALIZER_INTERFACE_CLASSES = Sets.newHashSet(SnapshotSerializer.class, PullSerializer.class);

    private final Client client;
    private final WebTarget rootTarget;
    private final WebTarget schemasTarget;
    private final WebTarget searchFieldsTarget;

    private final Options options;
    private final ClassLoaderCache classLoaderCache;
    private final SchemaVersionInfoCache schemaVersionInfoCache;

    public SchemaRegistryClient(Map<String, ?> conf) {
        options = new Options(conf);

        ClientConfig config = createClientConfig(conf);
        client = ClientBuilder.newBuilder().withConfig(config).build();
        client.register(MultiPartFeature.class);

        String rootCatalogURL = (String) conf.get(SCHEMA_REGISTRY_URL);
        rootTarget = client.target(rootCatalogURL);
        schemasTarget = rootTarget.path(SCHEMAS_PATH);
        searchFieldsTarget = schemasTarget.path("search/fields");

        classLoaderCache = new ClassLoaderCache(this);

        schemaVersionInfoCache = new SchemaVersionInfoCache(key -> _getSchema(key), options.getMaxSchemaCacheSize(), options.getSchemaExpiryInSecs());
    }

    protected ClientConfig createClientConfig(Map<String, ?> conf) {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        config.property(ClientProperties.FOLLOW_REDIRECTS, true);
        for (Map.Entry<String, ?> entry : conf.entrySet()) {
            config.property(entry.getKey(), entry.getValue());
        }
        return config;
    }

    public Options getOptions() {
        return options;
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public boolean registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        return postEntity(schemasTarget, schemaMetadata, Boolean.class);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return getEntity(schemasTarget.path(schemaName), SchemaMetadataInfo.class);
    }

    @Override
    public Integer addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException {
        //create schemainfo
        boolean success = registerSchemaMetadata(schemaMetadata);
        if (!success) {
            throw new RuntimeException("Given SchemaInfo could not be registered.");
        }

        // add version
        return addSchemaVersion(schemaMetadata.getName(), schemaVersion);
    }

    @Override
    public Integer addSchemaVersion(String schemaName, SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException {
        WebTarget target = schemasTarget.path(schemaName).path("/versions");
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaVersion), Response.class);
        int status = response.getStatus();
        String msg = response.readEntity(String.class);
        if (status == Response.Status.BAD_REQUEST.getStatusCode() || status == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            CatalogResponse catalogResponse = readCatalogResponse(msg);
            if (CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new IncompatibleSchemaException(catalogResponse.getResponseMessage());
            } else if (CatalogResponse.ResponseMessage.INVALID_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new InvalidSchemaException(catalogResponse.getResponseMessage());
            } else {
                throw new RuntimeException(catalogResponse.getResponseMessage());
            }

        }

        return readEntity(msg, Integer.class);
    }

    private CatalogResponse readCatalogResponse(String msg) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode node = objectMapper.readTree(msg);

            return objectMapper.treeToValue(node, CatalogResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        try {
            return schemaVersionInfoCache.getSchema(schemaVersionKey);
        } catch (SchemaNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private SchemaVersionInfo _getSchema(SchemaVersionKey schemaVersionKey) {
        String schemaName = schemaVersionKey.getSchemaName();
        WebTarget webTarget = schemasTarget.path(String.format("%s/versions/%d", schemaName, schemaVersionKey.getVersion()));

        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(schemaName + "/versions/latest");
        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(schemaName + "/versions");
        return getEntities(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(schemaName + "/compatibility");
        String response = webTarget.request().post(Entity.text(toSchemaText), String.class);
        return readEntity(response, Boolean.class);
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
        WebTarget target = searchFieldsTarget;
        for (Map.Entry<String, String> entry : schemaFieldQuery.toQueryMap().entrySet()) {
            target = target.queryParam(entry.getKey(), entry.getValue());
        }

        return getEntities(target, SchemaVersionKey.class);
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
        return rootTarget.path(FILES_PATH).path("download/" + fileId).request().get(InputStream.class);
    }

    @Override
    public Long addSerializer(SerDesInfo serializerInfo) {
        return postEntity(rootTarget.path(SERIALIZERS_PATH), serializerInfo, Long.class);
    }

    @Override
    public Long addDeserializer(SerDesInfo deserializerInfo) {
        return postEntity(rootTarget.path(DESERIALIZERS_PATH), deserializerInfo, Long.class);
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        String path = String.format("%s/mapping/%s", schemaName, serDesId.toString());

        Boolean success = postEntity(schemasTarget.path(path), null, Boolean.class);
        LOG.info("Received response while mapping schema [{}] with serialzer/deserializer [{}] : [{}]", schemaName, serDesId, success);
    }

    @Override
    public Collection<SerDesInfo> getSerializers(String schemaName) {
        String path = schemaName + "/serializers/";
        return getEntities(schemasTarget.path(path), SerDesInfo.class);
    }

    @Override
    public Collection<SerDesInfo> getDeserializers(String schemaName) {
        String path = schemaName + "/deserializers/";
        return getEntities(schemasTarget.path(path), SerDesInfo.class);
    }

    public <T> T createSerializerInstance(SerDesInfo serializerInfo) {
        return createInstance(serializerInfo, SERIALIZER_INTERFACE_CLASSES);
    }

    @Override
    public <T> T createDeserializerInstance(SerDesInfo deserializerInfo) {
        return createInstance(deserializerInfo, DESERIALIZER_INTERFACE_CLASSES);
    }

    private <T> T createInstance(SerDesInfo serDesInfo, Set<Class<?>> interfaceClasses) {
        if (interfaceClasses == null || interfaceClasses.isEmpty()) {
            throw new IllegalArgumentException("interfaceClasses array must be neither null nor empty.");
        }

        // loading serializer, create a class loader and and keep them in cache.
        String fileId = serDesInfo.getFileId();
        // get class loader for this file ID
        ClassLoader classLoader = classLoaderCache.getClassLoader(fileId);

        T t;
        try {
            String className = serDesInfo.getClassName();
            Class<T> clazz = (Class<T>) Class.forName(className, true, classLoader);
            t = clazz.newInstance();
            List<Class<?>> classes = new ArrayList<>();
            for (Class<?> interfaceClass : interfaceClasses) {
                if (interfaceClass.isAssignableFrom(clazz)) {
                    classes.add(interfaceClass);
                }
            }

            if (classes.isEmpty()) {
                throw new RuntimeException("Given Serialize/Deserializer " + className + " class does not implement any " +
                        "one of the registered interfaces: " + interfaceClasses);
            }

            Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                    classes.toArray(new Class[classes.size()]),
                    new ClassLoaderAwareInvocationHandler(classLoader, t));
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new SerDesException(e);
        }

        return t;
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

    public static class Options {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
        public static final String LOCAL_JAR_PATH = "schema.registry.client.local.jars.path";
        public static final String CLASSLOADER_CACHE_SIZE = "schema.registry.client.class.loader.cache.size";
        public static final String CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = "schema.registry.client.class.loader.cache.expiry.interval";
        public static final int DEFAULT_CLASS_LOADER_CACHE_SIZE = 1024;
        public static final long DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60;
        public static final String DEFAULT_LOCAL_JARS_PATH = "/tmp/schema-registry/local-jars";
        public static final String SCHEMA_CACHE_SIZE = "schema.registry.client.schema.cache.size";
        public static final String SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = "schema.registry.client.schema.cache.expiry.interval";
        public static final int DEFAULT_SCHEMA_CACHE_SIZE = 1024;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 5 * 60;

        // connection properties
        public static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;
        public static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

        private final Map<String, ?> config;

        public Options(Map<String, ?> config) {
            this.config = config;
        }

        private Object getPropertyValue(String propertyKey, Object defaultValue) {
            Object value = config.get(propertyKey);
            return value != null ? value : defaultValue;
        }

        public int getClassLoaderCacheSize() {
            Integer value = (Integer) getPropertyValue(CLASSLOADER_CACHE_SIZE, DEFAULT_CLASS_LOADER_CACHE_SIZE);
            checkPositiveNumber(value);
            return value;
        }

        public long getClassLoaderCacheExpiryInSecs() {
            Long value = (Long) getPropertyValue(CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS);
            checkPositiveNumber(value);
            return value;
        }

        public String getLocalJarPath() {
            return (String) getPropertyValue(LOCAL_JAR_PATH, DEFAULT_LOCAL_JARS_PATH);
        }

        public int getMaxSchemaCacheSize() {
            Integer value = (Integer) getPropertyValue(SCHEMA_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE);
            checkPositiveNumber(value);
            return value;
        }

        public long getSchemaExpiryInSecs() {
            Long value = (Long) getPropertyValue(SCHEMA_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS);
            checkPositiveNumber(value);
            return value;
        }

        private void checkPositiveNumber(Number number) {
            if (number.doubleValue() <= 0) {
                throw new IllegalArgumentException("Given value must be a positive number");
            }
        }

    }
}
