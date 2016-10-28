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
package org.apache.registries.schemaregistry.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import org.apache.registries.common.catalog.CatalogResponse;
import org.apache.registries.common.util.ClassLoaderAwareInvocationHandler;
import org.apache.registries.schemaregistry.SchemaFieldQuery;
import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.SchemaMetadataInfo;
import org.apache.registries.schemaregistry.SchemaVersion;
import org.apache.registries.schemaregistry.SchemaVersionInfo;
import org.apache.registries.schemaregistry.SchemaVersionInfoCache;
import org.apache.registries.schemaregistry.SchemaVersionKey;
import org.apache.registries.schemaregistry.SerDesInfo;
import org.apache.registries.schemaregistry.errors.IncompatibleSchemaException;
import org.apache.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.registries.schemaregistry.serde.SerDesException;
import org.apache.registries.schemaregistry.serde.SnapshotDeserializer;
import org.apache.registries.schemaregistry.serde.SnapshotSerializer;
import org.apache.registries.schemaregistry.serde.pull.PullDeserializer;
import org.apache.registries.schemaregistry.serde.pull.PullSerializer;
import org.apache.registries.schemaregistry.serde.push.PushDeserializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Proxy;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.registries.schemaregistry.client.SchemaRegistryClient.Options.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.registries.schemaregistry.client.SchemaRegistryClient.Options.DEFAULT_READ_TIMEOUT;
import static org.apache.registries.schemaregistry.client.SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL;

/**
 * This is the default implementation of {@link ISchemaRegistryClient} which connects to the given {@code rootCatalogURL}.
 * <p>
 * An instance of SchemaRegistryClient can be instantiated by passing configuration properties like below.
 * <pre>
 *     SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);
 * </pre>
 *
 * There are different options available as mentioned in {@link Options} like
 * <pre>
 * - {@link Options#SCHEMA_REGISTRY_URL}.
 * - {@link Options#SCHEMA_METADATA_CACHE_SIZE}.
 * - {@link Options#SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Options#SCHEMA_VERSION_CACHE_SIZE}.
 * - {@link Options#SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Options#SCHEMA_TEXT_CACHE_SIZE}.
 * - {@link Options#SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS}.
 *
 * and many other properties like {@link ClientProperties}
 * </pre>
 * <pre>
 * This can be used to
 *      - register schema metadatas
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
    private static final Set<Class<?>> DESERIALIZER_INTERFACE_CLASSES = Sets.<Class<?>>newHashSet(SnapshotDeserializer.class, PullDeserializer.class, PushDeserializer.class);
    private static final Set<Class<?>> SERIALIZER_INTERFACE_CLASSES = Sets.<Class<?>>newHashSet(SnapshotSerializer.class, PullSerializer.class);

    private final Client client;
    private final WebTarget rootTarget;
    private final WebTarget schemasTarget;
    private final WebTarget searchFieldsTarget;

    private final Options options;
    private final ClassLoaderCache classLoaderCache;
    private final SchemaVersionInfoCache schemaVersionInfoCache;
    private final LoadingCache<String, SchemaMetadataInfo> schemaMetadataCache;
    private final Cache<SchemaDigestEntry, Integer> schemaTextCache;

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

        schemaVersionInfoCache = new SchemaVersionInfoCache(new SchemaVersionInfoCache.SchemaRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                return _getSchema(key);
            }
        }, options.getMaxSchemaVersionCacheSize(), options.getSchemaVersionCacheExpiryInSecs());

        schemaMetadataCache = CacheBuilder.newBuilder()
                .maximumSize(options.getMaxSchemaMetadataCacheSize())
                .expireAfterAccess(options.getSchemaMetadataCacheExpiryInSecs(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<String, SchemaMetadataInfo>() {
                    @Override
                    public SchemaMetadataInfo load(String schemaName) throws Exception {
                        return getEntity(schemasTarget.path(schemaName), SchemaMetadataInfo.class);
                    }
                });

        schemaTextCache = CacheBuilder.newBuilder()
                .maximumSize(options.getMaxSchemaTextCacheSize())
                .expireAfterAccess(options.getSchemaTextCacheExpiryInSecs(), TimeUnit.MILLISECONDS)
                .build();
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
        SchemaMetadataInfo schemaMetadataInfo = schemaMetadataCache.getIfPresent(schemaMetadata.getName());
        if (schemaMetadataInfo == null) {
            return _registerSchemaMetadata(schemaMetadata, schemasTarget);
        }

        return true;
    }

    private Boolean _registerSchemaMetadata(SchemaMetadata schemaMetadata, WebTarget schemasTarget) {
        return postEntity(schemasTarget, schemaMetadata, Boolean.class);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        try {
            return schemaMetadataCache.get(schemaName);
        } catch (ExecutionException e) {
            LOG.error("Error occurred while retrieving schema metadata for [{}]", schemaName, e);
            Throwable cause = e.getCause();
            if (!(cause instanceof NotFoundException)) {
                throw new RuntimeException(cause.getMessage(), cause);
            }
        }
        return null;
    }

    @Override
    public Integer addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion) throws
            InvalidSchemaException, IncompatibleSchemaException {
        // get it, if it exists in cache
        SchemaDigestEntry schemaDigestEntry = buildSchemaTextEntry(schemaVersion, schemaMetadata.getName());
        Integer version = schemaTextCache.getIfPresent(schemaDigestEntry);
        if (version != null) {
            return version;
        }

        //register schema metadata if it does not exist
        boolean success = registerSchemaMetadata(schemaMetadata);
        if (!success) {
            LOG.error("Schema Metadata [{}] is not registered successfully", schemaMetadata);
            throw new RuntimeException("Given SchemaInfo could not be registered: " + schemaMetadata);
        }

        // add version
        version = addSchemaVersion(schemaMetadata.getName(), schemaVersion);
        return version;
    }

    private SchemaDigestEntry buildSchemaTextEntry(SchemaVersion schemaVersion, String name) {
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("MD5").digest(schemaVersion.getSchemaText().getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        // storing schema text string is expensive, so storing digest in cache's key.
        return new SchemaDigestEntry(name, digest);
    }

    @Override
    public Integer addSchemaVersion(final String schemaName, final SchemaVersion schemaVersion)
            throws InvalidSchemaException, IncompatibleSchemaException {

        try {
            return schemaTextCache.get(buildSchemaTextEntry(schemaVersion, schemaName), new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return doAddSchemaVersion(schemaName, schemaVersion);
                }
            });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOG.error("Encountered error while adding new version [{}] of schema [{}] and error [{}]", schemaVersion, schemaName, e);
            if (cause != null) {
                if (cause instanceof InvalidSchemaException)
                    throw (InvalidSchemaException) cause;
                else if (cause instanceof IncompatibleSchemaException) {
                    throw (IncompatibleSchemaException) cause;
                } else {
                    throw new RuntimeException(cause.getMessage(), cause);
                }
            } else {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    private Integer doAddSchemaVersion(String schemaName, SchemaVersion schemaVersion) throws IncompatibleSchemaException, InvalidSchemaException {
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

    public static final class Options {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        /**
         * URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1
         */
        public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

        /**
         * Local directory path to which downloaded jars should be copied to. For ex: /tmp/schema-registry/local-jars
         */
        public static final String LOCAL_JAR_PATH = "schema.registry.client.local.jars.path";

        /**
         * Maximum size of classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_SIZE}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final String CLASSLOADER_CACHE_SIZE = "schema.registry.client.class.loader.cache.size";

        /**
         * Expiry interval(in seconds) of an entry in classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final String CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = "schema.registry.client.class.loader.cache.expiry.interval";

        /**
         * Default value for classloader cache size.
         */
        public static final int DEFAULT_CLASSLOADER_CACHE_SIZE = 1024;

        /**
         * Default value for cache expiry interval in seconds.
         */
        public static final long DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60;

        /**
         * Default path for downloaded jars to be stored.
         */
        public static final String DEFAULT_LOCAL_JARS_PATH = "/tmp/schema-registry/local-jars";

        /**
         * Maximum size of schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final String SCHEMA_VERSION_CACHE_SIZE = "schema.registry.client.schema.version.cache.size";

        /**
         * Expiry interval(in seconds) of an entry in schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final String SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS = "schema.registry.client.schema.version.cache.expiry.interval";

        /**
         * Maximum size of schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final String SCHEMA_METADATA_CACHE_SIZE = "schema.registry.client.schema.metadata.cache.size";

        /**
         * Expiry interval(in seconds) of an entry in schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final String SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS = "schema.registry.client.schema.metadata.cache.expiry.interval";

        /**
         * Maximum size of schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}.
         * This cache has ability to store/get entries with same schema name and schema text.
         */
        public static final String SCHEMA_TEXT_CACHE_SIZE = "schema.registry.client.schema.text.cache.size";

        /**
         * Expiry interval(in seconds) of an entry in schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final String SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS = "schema.registry.client.schema.text.cache.expiry.interval";

        public static final int DEFAULT_SCHEMA_CACHE_SIZE = 1024;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 5 * 60;

        // connection properties
        /**
         * Default connection timeout on connections created while connecting to schema registry.
         */
        public static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;

        /**
         * Default read timeout on connections created while connecting to schema registry.
         */
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
            Integer value = (Integer) getPropertyValue(CLASSLOADER_CACHE_SIZE, DEFAULT_CLASSLOADER_CACHE_SIZE);
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

        public int getMaxSchemaVersionCacheSize() {
            Integer value = (Integer) getPropertyValue(SCHEMA_VERSION_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE);
            checkPositiveNumber(value);
            return value;
        }

        public long getSchemaVersionCacheExpiryInSecs() {
            Long value = (Long) getPropertyValue(SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS);
            checkPositiveNumber(value);
            return value;
        }

        public int getMaxSchemaTextCacheSize() {
            Integer value = (Integer) getPropertyValue(SCHEMA_TEXT_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE);
            checkPositiveNumber(value);
            return value;
        }

        public long getSchemaTextCacheExpiryInSecs() {
            Long value = (Long) getPropertyValue(SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS);
            checkPositiveNumber(value);
            return value;
        }

        public int getMaxSchemaMetadataCacheSize() {
            Integer value = (Integer) getPropertyValue(SCHEMA_METADATA_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE);
            checkPositiveNumber(value);
            return value;
        }

        public long getSchemaMetadataCacheExpiryInSecs() {
            Long value = (Long) getPropertyValue(SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS);
            checkPositiveNumber(value);
            return value;
        }

        private void checkPositiveNumber(Number number) {
            if (number.doubleValue() <= 0) {
                throw new IllegalArgumentException("Given value must be a positive number");
            }
        }

    }

    private static class SchemaDigestEntry {
        private final String name;
        private final byte[] schemaDigest;

        SchemaDigestEntry(String name, byte[] schemaDigest) {
            Preconditions.checkNotNull(name, "name can not be null");
            Preconditions.checkNotNull(schemaDigest, "schema digest can not be null");

            this.name = name;
            this.schemaDigest = schemaDigest;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SchemaDigestEntry that = (SchemaDigestEntry) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            return Arrays.equals(schemaDigest, that.schemaDigest);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(schemaDigest);
            return result;
        }
    }
}
