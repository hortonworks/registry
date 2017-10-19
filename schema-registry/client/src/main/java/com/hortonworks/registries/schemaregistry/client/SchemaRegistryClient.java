/**
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.hortonworks.registries.auth.KerberosLogin;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.util.ClassLoaderAwareInvocationHandler;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.ConfigEntry;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfoCache;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullDeserializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullSerializer;
import com.hortonworks.registries.schemaregistry.serde.push.PushDeserializer;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_CONNECTION_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_READ_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;

/**
 * This is the default implementation of {@link ISchemaRegistryClient} which connects to the given {@code rootCatalogURL}.
 * <p>
 * An instance of SchemaRegistryClient can be instantiated by passing configuration properties like below.
 * <pre>
 *     SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);
 * </pre>
 * <p>
 * There are different options available as mentioned in {@link Configuration} like
 * <pre>
 * - {@link Configuration#SCHEMA_REGISTRY_URL}.
 * - {@link Configuration#SCHEMA_METADATA_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Configuration#SCHEMA_VERSION_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Configuration#SCHEMA_TEXT_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS}.
 *
 * and many other properties like {@link ClientProperties}
 * </pre>
 * <pre>
 * This can be used to
 *      - register schema metadata
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
    private static final String SCHEMA_PROVIDERS_PATH = SCHEMA_REGISTRY_PATH + "/schemaproviders/";
    private static final String SCHEMAS_BY_ID_PATH = SCHEMA_REGISTRY_PATH + "/schemasById/";
    private static final String SCHEMA_VERSIONS_PATH = SCHEMAS_PATH + "versions/";
    private static final String FILES_PATH = SCHEMA_REGISTRY_PATH + "/files/";
    private static final String SERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/serdes/";
    private static final String REGISTY_CLIENT_JAAS_SECTION = "RegistryClient";
    private static final Set<Class<?>> DESERIALIZER_INTERFACE_CLASSES = Sets.<Class<?>>newHashSet(SnapshotDeserializer.class, PullDeserializer.class, PushDeserializer.class);
    private static final Set<Class<?>> SERIALIZER_INTERFACE_CLASSES = Sets.<Class<?>>newHashSet(SnapshotSerializer.class, PullSerializer.class);
    private static final String SEARCH_FIELDS = SCHEMA_REGISTRY_PATH + "/search/schemas/fields";
    private static Subject subject;

    static {
        String jaasConfigFile = System.getProperty("java.security.auth.login.config");
        if (jaasConfigFile != null && !jaasConfigFile.trim().isEmpty()) {
            KerberosLogin kerberosLogin = new KerberosLogin();
            kerberosLogin.configure(new HashMap<>(), REGISTY_CLIENT_JAAS_SECTION);
            try {
                subject = kerberosLogin.login().getSubject();
            } catch (LoginException e) {
                subject = null;
                LOG.error("Could not login using jaas config  section " + REGISTY_CLIENT_JAAS_SECTION);
            }
        } else {
            LOG.warn("System property for jaas config file is not defined. Its okay if schema registry is not running in secured mode");
            subject = null;
        }
    }

    private final Client client;
    private final UrlSelector urlSelector;
    private final Map<String, SchemaRegistryTargets> urlWithTargets;

    private final Configuration configuration;
    private final ClassLoaderCache classLoaderCache;
    private final SchemaVersionInfoCache schemaVersionInfoCache;
    private final SchemaMetadataCache schemaMetadataCache;
    private final Cache<SchemaDigestEntry, SchemaIdVersion> schemaTextCache;

    private static final String SSL_CONFIGURATION_KEY = "schema.registry.client.ssl";
    private static final String HOSTNAME_VERIFIER_CLASS_KEY = "hostnameVerifierClass";

    /**
     * Creates {@link SchemaRegistryClient} instance with the given yaml config.
     *
     * @param confFile config file which contains the configuration entries.
     *
     * @throws IOException when any IOException occurs while reading the given confFile
     */
    public SchemaRegistryClient(File confFile) throws IOException {
        this(buildConfFromFile(confFile));
    }

    private static Map<String, ?> buildConfFromFile(File confFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(confFile)) {
            return (Map<String, Object>) new Yaml().load(IOUtils.toString(fis, "UTF-8"));
        }
    }

    public SchemaRegistryClient(Map<String, ?> conf) {
        configuration = new Configuration(conf);

        ClientConfig config = createClientConfig(conf);
        ClientBuilder clientBuilder = ClientBuilder.newBuilder()
                                                   .withConfig(config)
                                                   .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE);
        if (conf.containsKey(SSL_CONFIGURATION_KEY)) {
            Map<String, String> sslConfigurations = (Map<String, String>) conf.get(SSL_CONFIGURATION_KEY);
            clientBuilder.sslContext(createSSLContext(sslConfigurations));
            if (sslConfigurations.containsKey(HOSTNAME_VERIFIER_CLASS_KEY)) {
                HostnameVerifier hostNameVerifier = null;
                String hostNameVerifierClassName = sslConfigurations.get(HOSTNAME_VERIFIER_CLASS_KEY);
                try {
                    hostNameVerifier = (HostnameVerifier) Class.forName(hostNameVerifierClassName).newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate hostNameVerifierClass : " + hostNameVerifierClassName, e);
                }
                clientBuilder.hostnameVerifier(hostNameVerifier);
            }
        }
        client = clientBuilder.build();
        client.register(MultiPartFeature.class);

        // get list of urls and create given or default UrlSelector.
        urlSelector = createUrlSelector();
        urlWithTargets = new ConcurrentHashMap<>();

        classLoaderCache = new ClassLoaderCache(this);

        schemaVersionInfoCache = new SchemaVersionInfoCache(new SchemaVersionRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                return doGetSchemaVersionInfo(key);
            }

            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                return doGetSchemaVersionInfo(key);
            }
        },
                                                            ((Number) configuration.getValue(Configuration.SCHEMA_VERSION_CACHE_SIZE
                                                                                                     .name())).intValue(),
                                                            ((Number) configuration.getValue(Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS
                                                                                                     .name())).longValue());

        SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher = createSchemaMetadataFetcher();
        schemaMetadataCache = new SchemaMetadataCache(((Number) configuration.getValue(Configuration.SCHEMA_METADATA_CACHE_SIZE
                                                                                               .name())).longValue(),
                                                      ((Number) configuration.getValue(Configuration.SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS
                                                                                               .name())).longValue(),
                                                      schemaMetadataFetcher);

        schemaTextCache = CacheBuilder.newBuilder()
                                      .maximumSize(((Number) configuration.getValue(Configuration.SCHEMA_TEXT_CACHE_SIZE
                                                                                            .name())).longValue())
                                      .expireAfterAccess(((Number) configuration.getValue(Configuration.SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS
                                                                                                  .name())).longValue(),
                                                         TimeUnit.MILLISECONDS)
                                      .build();
    }

    protected SSLContext createSSLContext(Map<String, String> sslConfigurations) {
        SslConfigurator sslConfigurator = SslConfigurator.newInstance();
        String keyPassword = "keyPassword";
        sslConfigurator.keyStoreType(sslConfigurations.get("keyStoreType"))
                       .keyStoreFile(sslConfigurations.get("keyStorePath"))
                       .keyStorePassword(sslConfigurations.get("keyStorePassword"))
                       .trustStoreType(sslConfigurations.get("trustStoreType"))
                       .trustStoreFile(sslConfigurations.get("trustStorePath"))
                       .trustStorePassword(sslConfigurations.get("trustStorePassword"))
                       .keyStoreProvider(sslConfigurations.get("keyStoreProvider"))
                       .trustStoreProvider(sslConfigurations.get("trustStoreProvider"))
                       .keyManagerFactoryAlgorithm(sslConfigurations.get("keyManagerFactoryAlgorithm"))
                       .keyManagerFactoryProvider(sslConfigurations.get("keyManagerFactoryProvider"))
                       .trustManagerFactoryAlgorithm(sslConfigurations.get("trustManagerFactoryAlgorithm"))
                       .trustManagerFactoryProvider(sslConfigurations.get("trustManagerFactoryProvider"))
                       .securityProtocol(sslConfigurations.get("protocol"));
        if (sslConfigurations.containsKey(keyPassword))
            sslConfigurator.keyPassword(sslConfigurations.get(keyPassword));
        return sslConfigurator.createSSLContext();
    }

    private SchemaRegistryTargets currentSchemaRegistryTargets() {
        String url = urlSelector.select();
        urlWithTargets.computeIfAbsent(url, s -> new SchemaRegistryTargets(client.target(s)));
        return urlWithTargets.get(url);
    }

    private static class SchemaRegistryTargets {
        private final WebTarget schemaProvidersTarget;
        private final WebTarget schemasTarget;
        private final WebTarget schemasByIdTarget;
        private final WebTarget rootTarget;
        private final WebTarget searchFieldsTarget;
        private final WebTarget serializersTarget;
        private final WebTarget filesTarget;
        private final WebTarget schemaVersionsTarget;
        private final WebTarget schemaVersionsByIdTarget;
        private final WebTarget schemaVersionsStatesMachineTarget;

        SchemaRegistryTargets(WebTarget rootTarget) {
            this.rootTarget = rootTarget;
            schemaProvidersTarget = rootTarget.path(SCHEMA_PROVIDERS_PATH);
            schemasTarget = rootTarget.path(SCHEMAS_PATH);
            schemasByIdTarget = rootTarget.path(SCHEMAS_BY_ID_PATH);
            schemaVersionsByIdTarget = schemasTarget.path("versionsById");
            schemaVersionsTarget = rootTarget.path(SCHEMA_VERSIONS_PATH);
            schemaVersionsStatesMachineTarget = schemaVersionsTarget.path("statemachine");
            searchFieldsTarget = rootTarget.path(SEARCH_FIELDS);
            serializersTarget = rootTarget.path(SERIALIZERS_PATH);
            filesTarget = rootTarget.path(FILES_PATH);
        }

    }

    private UrlSelector createUrlSelector() {
        UrlSelector urlSelector = null;
        String rootCatalogURL = configuration.getValue(SCHEMA_REGISTRY_URL.name());
        String urlSelectorClass = configuration.getValue(Configuration.URL_SELECTOR_CLASS.name());
        if (urlSelectorClass == null) {
            urlSelector = new LoadBalancedFailoverUrlSelector(rootCatalogURL);
        } else {
            try {
                urlSelector = (UrlSelector) Class.forName(urlSelectorClass)
                                                 .getConstructor(String.class)
                                                 .newInstance(rootCatalogURL);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        urlSelector.init(configuration.getConfig());

        return urlSelector;
    }

    private SchemaMetadataCache.SchemaMetadataFetcher createSchemaMetadataFetcher() {
        return new SchemaMetadataCache.SchemaMetadataFetcher() {
            @Override
            public SchemaMetadataInfo fetch(String name) throws SchemaNotFoundException {
                try {
                    return getEntity(currentSchemaRegistryTargets().schemasTarget.path(name), SchemaMetadataInfo.class);
                } catch (NotFoundException e) {
                    throw new SchemaNotFoundException(e);
                }
            }

            @Override
            public SchemaMetadataInfo fetch(Long id) throws SchemaNotFoundException {
                try {
                    return getEntity(currentSchemaRegistryTargets().schemasByIdTarget.path(id.toString()), SchemaMetadataInfo.class);
                } catch (NotFoundException e) {
                    throw new SchemaNotFoundException(e);
                }
            }
        };
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

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return getEntities(currentSchemaRegistryTargets().schemaProvidersTarget, SchemaProviderInfo.class);
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        return addSchemaMetadata(schemaMetadata);
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
        SchemaMetadataInfo schemaMetadataInfo = schemaMetadataCache.getIfPresent(SchemaMetadataCache.Key.of(schemaMetadata
                                                                                                                    .getName()));
        if (schemaMetadataInfo == null) {
            return doRegisterSchemaMetadata(schemaMetadata, currentSchemaRegistryTargets().schemasTarget);
        }

        return schemaMetadataInfo.getId();
    }

    @Override
    public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        SchemaMetadataInfo schemaMetadataInfo = postEntity(currentSchemaRegistryTargets().schemasTarget.path(schemaName), schemaMetadata, SchemaMetadataInfo.class);
        if (schemaMetadataInfo != null) {
            schemaMetadataCache.put(SchemaMetadataCache.Key.of(schemaName), schemaMetadataInfo);
        }
        return schemaMetadataInfo;
    }


    private Long doRegisterSchemaMetadata(SchemaMetadata schemaMetadata, WebTarget schemasTarget) {
        return postEntity(schemasTarget, schemaMetadata, Long.class);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaMetadataCache.get(SchemaMetadataCache.Key.of(schemaName));
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return schemaMetadataCache.get(SchemaMetadataCache.Key.of(schemaMetadataId));
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion) throws
                                                                                                        InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
        // get it, if it exists in cache
        SchemaDigestEntry schemaDigestEntry = buildSchemaTextEntry(schemaVersion, schemaMetadata.getName());
        SchemaIdVersion schemaIdVersion = schemaTextCache.getIfPresent(schemaDigestEntry);

        if (schemaIdVersion == null) {
            //register schema metadata if it does not exist
            Long metadataId = registerSchemaMetadata(schemaMetadata);
            if (metadataId == null) {
                LOG.error("Schema Metadata [{}] is not registered successfully", schemaMetadata);
                throw new RuntimeException("Given SchemaMetadata could not be registered: " + schemaMetadata);
            }

            // add schemaIdVersion
            schemaIdVersion = addSchemaVersion(schemaMetadata.getName(), schemaVersion);
        }

        return schemaIdVersion;
    }

    public SchemaIdVersion uploadSchemaVersion(final String schemaName,
                                               final String description,
                                               final InputStream schemaVersionInputStream)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Schema with name " + schemaName + " not found");
        }

        StreamDataBodyPart streamDataBodyPart = new StreamDataBodyPart("file", schemaVersionInputStream);

        WebTarget target = currentSchemaRegistryTargets().schemasTarget.path(schemaName).path("/versions/upload");
        MultiPart multipartEntity =
                new FormDataMultiPart()
                        .field("description", description, MediaType.APPLICATION_JSON_TYPE)
                        .bodyPart(streamDataBodyPart);

        Entity<MultiPart> multiPartEntity = Entity.entity(multipartEntity, MediaType.MULTIPART_FORM_DATA);
        Response response = Subject.doAs(subject, new PrivilegedAction<Response>() {
            @Override
            public Response run() {
                return target.request().post(multiPartEntity, Response.class);
            }
        });
        return handleSchemaIdVersionResponse(schemaMetadataInfo, response);
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
    public SchemaIdVersion addSchemaVersion(final String schemaName, final SchemaVersion schemaVersion)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {

        try {
            return schemaTextCache.get(buildSchemaTextEntry(schemaVersion, schemaName),
                                       () -> doAddSchemaVersion(schemaName, schemaVersion));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOG.error("Encountered error while adding new version [{}] of schema [{}] and error [{}]", schemaVersion, schemaName, e);
            if (cause != null) {
                if (cause instanceof InvalidSchemaException)
                    throw (InvalidSchemaException) cause;
                else if (cause instanceof IncompatibleSchemaException) {
                    throw (IncompatibleSchemaException) cause;
                } else if (cause instanceof SchemaNotFoundException) {
                    throw (SchemaNotFoundException) cause;
                } else {
                    throw new RuntimeException(cause.getMessage(), cause);
                }
            } else {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        schemaVersionInfoCache.invalidateSchema(new SchemaVersionInfoCache.Key(schemaVersionKey));

        WebTarget target = currentSchemaRegistryTargets().schemasTarget.path(String.format("%s/versions/%s", schemaVersionKey
                .getSchemaName(), schemaVersionKey.getVersion()));
        Response response = Subject.doAs(subject, new PrivilegedAction<Response>() {
            @Override
            public Response run() {
                return target.request(MediaType.APPLICATION_JSON_TYPE).delete(Response.class);
            }
        });

        handleDeleteSchemaResponse(response);
    }

    private void handleDeleteSchemaResponse(Response response) throws SchemaNotFoundException {
        String msg = response.readEntity(String.class);
        switch (Response.Status.fromStatusCode(response.getStatus())) {
            case NOT_FOUND:
                throw new SchemaNotFoundException(msg);
            case INTERNAL_SERVER_ERROR:
                throw new RuntimeException(msg);
        }
    }

    private SchemaIdVersion doAddSchemaVersion(String schemaName,
                                               SchemaVersion schemaVersion) throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Schema with name " + schemaName + " not found");
        }

        WebTarget target = currentSchemaRegistryTargets().schemasTarget.path(schemaName).path("/versions");
        Response response = Subject.doAs(subject, new PrivilegedAction<Response>() {
            @Override
            public Response run() {
                return target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaVersion), Response.class);
            }
        });
        return handleSchemaIdVersionResponse(schemaMetadataInfo, response);
    }

    private SchemaIdVersion handleSchemaIdVersionResponse(SchemaMetadataInfo schemaMetadataInfo,
                                                          Response response) throws IncompatibleSchemaException, InvalidSchemaException {
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

        Integer version = readEntity(msg, Integer.class);

        SchemaVersionInfo schemaVersionInfo = doGetSchemaVersionInfo(new SchemaVersionKey(schemaMetadataInfo.getSchemaMetadata()
                                                                                                            .getName(), version));

        return new SchemaIdVersion(schemaMetadataInfo.getId(), version, schemaVersionInfo.getId());
    }

    public static CatalogResponse readCatalogResponse(String msg) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode node = objectMapper.readTree(msg);

            return objectMapper.treeToValue(node, CatalogResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        try {
            return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion));
        } catch (SchemaNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        try {
            return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaVersionKey));
        } catch (SchemaNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private SchemaVersionInfo doGetSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        if (schemaIdVersion.getSchemaVersionId() != null) {
            LOG.info("Getting schema version from target registry for [{}]", schemaIdVersion.getSchemaVersionId());
            return getEntity(currentSchemaRegistryTargets()
                                     .schemaVersionsByIdTarget
                                     .path(schemaIdVersion.getSchemaVersionId().toString()),
                             SchemaVersionInfo.class);
        } else if (schemaIdVersion.getSchemaMetadataId() != null) {
            SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaIdVersion.getSchemaMetadataId());
            SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadataInfo.getSchemaMetadata()
                                                                                       .getName(), schemaIdVersion.getVersion());
            LOG.info("Getting schema version from target registry for key [{}]", schemaVersionKey);
            return doGetSchemaVersionInfo(schemaVersionKey);
        }

        throw new IllegalArgumentException("Given argument not valid: " + schemaIdVersion);
    }

    private SchemaVersionInfo doGetSchemaVersionInfo(SchemaVersionKey schemaVersionKey) {
        LOG.info("Getting schema version from target registry for [{}]", schemaVersionKey);
        String schemaName = schemaVersionKey.getSchemaName();
        WebTarget webTarget = currentSchemaRegistryTargets().schemasTarget.path(String.format("%s/versions/%d", schemaName, schemaVersionKey
                .getVersion()));

        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        WebTarget webTarget = currentSchemaRegistryTargets().schemasTarget.path(encode(schemaName) + "/versions/latest");
        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    private static String encode(String schemaName) {
        try {
            return URLEncoder.encode(schemaName, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId)
            throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException {
        try {
            transitionSchemaVersionState(schemaVersionId, "enable");
        } catch (SchemaLifecycleException e) {
            Throwable cause = e.getCause();
            if (cause != null && cause instanceof IncompatibleSchemaException) {
                throw (IncompatibleSchemaException) cause;
            }
            throw e;
        }
    }

    @Override
    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "disable");
    }

    @Override
    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "delete");
    }

    @Override
    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "archive");
    }

    @Override
    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "startReview");
    }

    @Override
    public void transitionState(Long schemaVersionId,
                                Byte targetStateId) throws SchemaNotFoundException, SchemaLifecycleException {
        boolean result = transitionSchemaVersionState(schemaVersionId, targetStateId.toString());
    }

    @Override
    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        return getEntity(currentSchemaRegistryTargets().schemaVersionsStatesMachineTarget,
                         SchemaVersionLifecycleStateMachineInfo.class);
    }

    private boolean transitionSchemaVersionState(Long schemaVersionId,
                                                 String operationOrTargetState) throws SchemaNotFoundException, SchemaLifecycleException {

        WebTarget webTarget = currentSchemaRegistryTargets().schemaVersionsTarget.path(schemaVersionId + "/state/" + operationOrTargetState);
        Response response = Subject.doAs(subject, new PrivilegedAction<Response>() {
            @Override
            public Response run() {
                return webTarget.request().post(null);
            }
        });

        boolean result = handleSchemaLifeCycleResponse(response);

        // invalidate this entry from cache.
        schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId)));

        return result;
    }

    private boolean handleSchemaLifeCycleResponse(Response response) throws SchemaNotFoundException, SchemaLifecycleException {
        boolean result;
        int status = response.getStatus();
        if (status == Response.Status.OK.getStatusCode()) {
            result = response.readEntity(Boolean.class);
        } else if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class));
        } else if (status == Response.Status.BAD_REQUEST.getStatusCode()) {
            CatalogResponse catalogResponse = readCatalogResponse(response.readEntity(String.class));
            if (catalogResponse.getResponseCode() == CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA.getCode()) {
                throw new SchemaLifecycleException(new IncompatibleSchemaException(catalogResponse.getResponseMessage()));
            }
            throw new SchemaLifecycleException(catalogResponse.getResponseMessage());

        } else {
            throw new RuntimeException(response.readEntity(String.class));
        }

        return result;
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        WebTarget webTarget = currentSchemaRegistryTargets().schemasTarget.path(encode(schemaName) + "/versions");
        return getEntities(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaName,
                                                  String toSchemaText) throws SchemaNotFoundException {
        WebTarget webTarget = currentSchemaRegistryTargets().schemasTarget.path(encode(schemaName) + "/compatibility");
        String response = Subject.doAs(subject, new PrivilegedAction<String>() {
            @Override
            public String run() {
                return webTarget.request().post(Entity.text(toSchemaText), String.class);
            }
        });
        return readEntity(response, CompatibilityResult.class);
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException {
        return checkCompatibility(schemaName, toSchemaText).isCompatible();
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
        WebTarget target = currentSchemaRegistryTargets().searchFieldsTarget;
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
        return Subject.doAs(subject, new PrivilegedAction<String>() {
            @Override
            public String run() {
                return currentSchemaRegistryTargets().filesTarget.request()
                                                                 .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA), String.class);
            }
        });
    }

    @Override
    public InputStream downloadFile(String fileId) {
        return Subject.doAs(subject, new PrivilegedAction<InputStream>() {
            @Override
            public InputStream run() {
                return currentSchemaRegistryTargets().filesTarget.path("download/" + encode(fileId))
                                                                 .request()
                                                                 .get(InputStream.class);
            }
        });
    }

    @Override
    public Long addSerDes(SerDesPair serDesPair) {
        return postEntity(currentSchemaRegistryTargets().serializersTarget, serDesPair, Long.class);
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        String path = String.format("%s/mapping/%s", encode(schemaName), serDesId.toString());

        Boolean success = postEntity(currentSchemaRegistryTargets().schemasTarget.path(path), null, Boolean.class);
        LOG.info("Received response while mapping schema [{}] with serialzer/deserializer [{}] : [{}]", schemaName, serDesId, success);
    }

    @Override
    public <T> T getDefaultSerializer(String type) throws SerDesException {
        Collection<SchemaProviderInfo> supportedSchemaProviders = getSupportedSchemaProviders();
        for (SchemaProviderInfo schemaProvider : supportedSchemaProviders) {
            if (schemaProvider.getType().equals(type)) {
                try {
                    return (T) Class.forName(schemaProvider.getDefaultSerializerClassName()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new SerDesException(e);
                }
            }
        }

        throw new IllegalArgumentException("No schema provider registered for the given type " + type);
    }

    @Override
    public <T> T getDefaultDeserializer(String type) throws SerDesException {
        Collection<SchemaProviderInfo> supportedSchemaProviders = getSupportedSchemaProviders();
        for (SchemaProviderInfo schemaProvider : supportedSchemaProviders) {
            if (schemaProvider.getType().equals(type)) {
                try {
                    return (T) Class.forName(schemaProvider.getDefaultDeserializerClassName()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new SerDesException(e);
                }
            }
        }

        throw new IllegalArgumentException("No schema provider registered for the given type " + type);
    }

    @Override
    public Collection<SerDesInfo> getSerDes(String schemaName) {
        String path = encode(schemaName) + "/serdes/";
        return getEntities(currentSchemaRegistryTargets().schemasTarget.path(path), SerDesInfo.class);
    }

    public <T> T createSerializerInstance(SerDesInfo serDesInfo) {
        return createInstance(serDesInfo, true);
    }

    @Override
    public <T> T createDeserializerInstance(SerDesInfo serDesInfo) {
        return createInstance(serDesInfo, false);
    }

    @Override
    public void close() {
        client.close();
    }

    private <T> T createInstance(SerDesInfo serDesInfo, boolean isSerializer) {
        Set<Class<?>> interfaceClasses = isSerializer ? SERIALIZER_INTERFACE_CLASSES : DESERIALIZER_INTERFACE_CLASSES;

        if (interfaceClasses == null || interfaceClasses.isEmpty()) {
            throw new IllegalArgumentException("interfaceClasses array must be neither null nor empty.");
        }

        // loading serializer, create a class loader and and keep them in cache.
        final SerDesPair serDesPair = serDesInfo.getSerDesPair();
        String fileId = serDesPair.getFileId();
        // get class loader for this file ID
        ClassLoader classLoader = classLoaderCache.getClassLoader(fileId);

        T t;
        try {
            String className =
                    isSerializer ? serDesPair.getSerializerClassName() : serDesPair.getDeserializerClassName();

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
        String response = Subject.doAs(subject, new PrivilegedAction<String>() {
            @Override
            public String run() {
                return target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
            }
        });
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
        String response = Subject.doAs(subject, new PrivilegedAction<String>() {
            @Override
            public String run() {
                return target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(json), String.class);
            }
        });
        return readEntity(response, responseType);
    }

    private <T> T readEntity(String response, Class<T> clazz) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response, clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private <T> T getEntity(WebTarget target, Class<T> clazz) {
        String response = Subject.doAs(subject, new PrivilegedAction<String>() {
            @Override
            public String run() {
                return target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
            }
        });

        return readEntity(response, clazz);
    }

    public static final class Configuration {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        /**
         * URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1
         */
        public static final ConfigEntry<String> SCHEMA_REGISTRY_URL =
                ConfigEntry.mandatory("schema.registry.url",
                                      String.class,
                                      "URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1",
                                      "http://localhost:9090/api/v1",
                                      ConfigEntry.NonEmptyStringValidator.get());

        /**
         * Default path for downloaded jars to be stored.
         */
        public static final String DEFAULT_LOCAL_JARS_PATH = "/tmp/schema-registry/local-jars";

        /**
         * Local directory path to which downloaded jars should be copied to. For ex: /tmp/schema-registry/local-jars
         */
        public static final ConfigEntry<String> LOCAL_JAR_PATH =
                ConfigEntry.optional("schema.registry.client.local.jars.path",
                                     String.class,
                                     "URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1",
                                     DEFAULT_LOCAL_JARS_PATH,
                                     ConfigEntry.NonEmptyStringValidator.get());

        /**
         * Default value for classloader cache size.
         */
        public static final long DEFAULT_CLASSLOADER_CACHE_SIZE = 1024L;

        /**
         * Default value for cache expiry interval in seconds.
         */
        public static final long DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60L;

        /**
         * Maximum size of classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_SIZE}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final ConfigEntry<Number> CLASSLOADER_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.class.loader.cache.size",
                                     Integer.class,
                                     "Maximum size of classloader cache",
                                     DEFAULT_CLASSLOADER_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final ConfigEntry<Number> CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.class.loader.cache.expiry.interval.secs",
                                     Integer.class,
                                     "Expiry interval(in seconds) of an entry in classloader cache",
                                     DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        public static final long DEFAULT_SCHEMA_CACHE_SIZE = 1024;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 5 * 60L;

        /**
         * Maximum size of schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final ConfigEntry<Number> SCHEMA_VERSION_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.schema.version.cache.size",
                                     Integer.class,
                                     "Maximum size of schema version cache",
                                     DEFAULT_SCHEMA_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.schema.version.cache.expiry.interval.secs",
                                     Integer.class,
                                     "Expiry interval(in seconds) of an entry in schema version cache",
                                     DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Maximum size of schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final ConfigEntry<Number> SCHEMA_METADATA_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.schema.metadata.cache.size",
                                     Integer.class,
                                     "Maximum size of schema metadata cache",
                                     DEFAULT_SCHEMA_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.schema.metadata.cache.expiry.interval.secs",
                                     Integer.class,
                                     "Expiry interval(in seconds) of an entry in schema metadata cache",
                                     DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Maximum size of schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}.
         * This cache has ability to store/get entries with same schema name and schema text.
         */
        public static final ConfigEntry<Number> SCHEMA_TEXT_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.schema.text.cache.size",
                                     Integer.class,
                                     "Maximum size of schema text cache",
                                     DEFAULT_SCHEMA_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.schema.text.cache.expiry.interval.secs",
                                     Integer.class,
                                     "Expiry interval(in seconds) of an entry in schema text cache.",
                                     DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         *
         */
        public static final ConfigEntry<String> URL_SELECTOR_CLASS =
                ConfigEntry.optional("schema.registry.client.url.selector",
                                     String.class,
                                     "Schema Registry URL selector class.",
                                     FailoverUrlSelector.class.getName(),
                                     ConfigEntry.NonEmptyStringValidator.get());

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
        private final Map<String, ConfigEntry<?>> options;

        public Configuration(Map<String, ?> config) {
            Field[] fields = this.getClass().getDeclaredFields();
            this.options = Collections.unmodifiableMap(buildOptions(fields));
            this.config = buildConfig(config);
        }

        private Map<String, ?> buildConfig(Map<String, ?> config) {
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<String, ?> entry : config.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                ConfigEntry configEntry = options.get(key);
                if (configEntry != null) {
                    if (value != null) {
                        configEntry.validator().validate((value));
                    } else {
                        value = configEntry.defaultValue();
                    }
                }
                result.put(key, value);
            }

            return result;
        }

        private Map<String, ConfigEntry<?>> buildOptions(Field[] fields) {
            Map<String, ConfigEntry<?>> options = new HashMap<>();
            for (Field field : fields) {
                Class<?> type = field.getType();

                if (type.isAssignableFrom(ConfigEntry.class)) {
                    field.setAccessible(true);
                    try {
                        ConfigEntry configEntry = (ConfigEntry) field.get(this);
                        options.put(configEntry.name(), configEntry);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return options;
        }

        public <T> T getValue(String propertyKey) {
            return (T) (config.containsKey(propertyKey) ? config.get(propertyKey)
                                                        : options.get(propertyKey).defaultValue());
        }

        public Map<String, Object> getConfig() {
            return Collections.unmodifiableMap(config);
        }

        public Collection<ConfigEntry<?>> getAvailableConfigEntries() {
            return options.values();
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
