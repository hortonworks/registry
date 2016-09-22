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
import com.hortonworks.iotas.common.catalog.CatalogResponse;
import com.hortonworks.iotas.common.util.ClassLoaderAwareInvocationHandler;
import com.hortonworks.registries.schemaregistry.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfoCache;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullDeserializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullSerializer;
import com.hortonworks.registries.schemaregistry.serde.push.PushDeserializer;
import org.glassfish.jersey.client.ClientConfig;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL;

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
    private SchemaVersionInfoCache schemaVersionInfoCache;

    public SchemaRegistryClient(Map<String, ?> conf) {
        options = new Options(conf);

        client = ClientBuilder.newClient(new ClientConfig());
        client.register(MultiPartFeature.class);
        String rootCatalogURL = (String) conf.get(SCHEMA_REGISTRY_URL);
        rootTarget = client.target(rootCatalogURL);
        schemasTarget = rootTarget.path(SCHEMAS_PATH);
        searchFieldsTarget = schemasTarget.path("search/fields");

        classLoaderCache = new ClassLoaderCache(this);

        schemaVersionInfoCache = new SchemaVersionInfoCache(key -> _getSchema(key), options.getMaxSchemaCacheSize(), options.getSchemaExpiryInMillis());
    }

    public Options getOptions() {
        return options;
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public boolean registerSchemaInfo(SchemaInfo schemaInfo) {
        return postEntity(schemasTarget, schemaInfo, Boolean.class);
    }

    @Override
    public SchemaInfo getSchemaInfo(SchemaKey schemaKey) {
        return getEntity(schemaInfoPath(schemaKey), SchemaInfo.class);
    }

    @Override
    public Integer addSchemaVersion(SchemaInfo schemaInfo, SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException {
        SchemaKey schemaKey = schemaInfo.getSchemaKey();
        SchemaInstanceDetails schemaInstanceDetails = new SchemaInstanceDetails(schemaInfo.getDescription(), schemaInfo.getCompatibility(), schemaVersion);
        return addSchemaVersion(schemaKey, schemaInstanceDetails);
    }

    private WebTarget schemaInfoPath(SchemaKey schemaKey) {
        return schemasTarget.path(
                String.format("types/%s/groups/%s/names/%s",
                        schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName()));
    }

    @Override
    public Integer addSchemaVersion(SchemaKey schemaKey, SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException {
        SchemaInstanceDetails schemaInstanceDetails = new SchemaInstanceDetails(schemaVersion);
        return addSchemaVersion(schemaKey, schemaInstanceDetails);
    }

    private Integer addSchemaVersion(SchemaKey schemaKey, SchemaInstanceDetails schemaInstanceDetails) throws IncompatibleSchemaException, InvalidSchemaException {
        WebTarget target = schemaInfoPath(schemaKey);
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaInstanceDetails), Response.class);
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
        SchemaKey schemaKey = schemaVersionKey.getSchemaKey();
        WebTarget webTarget = schemasTarget.path(
                String.format("types/%s/groups/%s/names/%s/versions/%d",
                        schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName(), schemaVersionKey.getVersion()));

        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(SchemaKey schemaKey) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(
                String.format("types/%s/groups/%s/names/%s/versions/latest",
                        schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName()));
        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(SchemaKey schemaKey) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(
                String.format("types/%s/groups/%s/names/%s/versions",
                        schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName()));
        return getEntities(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public boolean isCompatibleWithAllVersions(SchemaKey schemaKey, String toSchemaText) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(
                String.format("types/%s/groups/%s/names/%s/compatibility",
                        schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName()));
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
    public void mapSchemaWithSerDes(SchemaKey schemaKey, Long serDesId) {
        String path = String.format("types/%s/groups/%s/names/%s/mapping/%s", schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName(), serDesId.toString());

        Boolean success = postEntity(schemasTarget.path(path), null, Boolean.class);
        LOG.info("Received response while mapping schemaMetadataKey [{}] with serialzer/deserializer [{}] : [{}]", schemaKey, serDesId, success);
    }

    @Override
    public Collection<SerDesInfo> getSerializers(SchemaKey schemaKey) {
        String path = String.format("types/%s/groups/%s/names/%s/serializers/", schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName());
        return getEntities(schemasTarget.path(path), SerDesInfo.class);
    }

    @Override
    public Collection<SerDesInfo> getDeserializers(SchemaKey schemaKey) {
        String path = String.format("types/%s/groups/%s/names/%s/deserializers/", schemaKey.getType(), schemaKey.getSchemaGroup(), schemaKey.getName());
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
                    classes.toArray(new Class[]{}),
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
        public static final String CLASSLOADER_CACHE_EXPIRY_INTERVAL_MILLISECS = "schema.registry.client.class.loader.cache.expiry.interval";
        public static final int DEFAULT_CLASS_LOADER_CACHE_SIZE = 1024;
        public static final long DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_MILLISECS = 60 * 60 * 1000L;
        public static final String DEFAULT_LOCAL_JARS_PATH = "/tmp/schema-registry/local-jars";
        public static final String SCHEMA_CACHE_SIZE = "schema.registry.client.schema.cache.size";
        public static final String SCHEMA_CACHE_EXPIRY_INTERVAL_MILLISECS = "schema.registry.client.schema.cache.expiry.interval";
        public static final int DEFAULT_SCHEMA_CACHE_SIZE = 1024;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_MILLISECS = 5 * 60 * 1000L;

        private final Map<String, ?> config;

        public Options(Map<String, ?> config) {
            this.config = config;
        }

        private Object getPropertyValue(String propertyKey, Object defaultValue) {
            Object value = config.get(propertyKey);
            return value != null ? value : defaultValue;
        }

        public int getClassLoaderCacheSize() {
            return (Integer) getPropertyValue(CLASSLOADER_CACHE_SIZE, DEFAULT_CLASS_LOADER_CACHE_SIZE);
        }

        public long getClassLoaderCacheExpiryInMilliSecs() {
            return (Long) getPropertyValue(CLASSLOADER_CACHE_EXPIRY_INTERVAL_MILLISECS, DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_MILLISECS);
        }

        public String getLocalJarPath() {
            return (String) getPropertyValue(LOCAL_JAR_PATH, DEFAULT_LOCAL_JARS_PATH);
        }

        public int getMaxSchemaCacheSize() {
            return (Integer) getPropertyValue(SCHEMA_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE);
        }

        public long getSchemaExpiryInMillis() {
            return (Long) getPropertyValue(SCHEMA_CACHE_EXPIRY_INTERVAL_MILLISECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_MILLISECS);
        }
    }
}
