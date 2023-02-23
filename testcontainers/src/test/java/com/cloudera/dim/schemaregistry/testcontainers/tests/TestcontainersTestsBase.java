/**
 * Copyright 2016-2023 Cloudera, Inc.
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

package com.cloudera.dim.schemaregistry.testcontainers.tests;

import com.cloudera.dim.schemaregistry.TestUtils;
import com.cloudera.dim.schemaregistry.config.RegistryYamlGenerator;
import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbConnProps;
import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbHandler;
import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbType;
import com.cloudera.dim.schemaregistry.testcontainers.env.envutils.LocalSchemaRegistryServerForTestcontainersTests;
import com.cloudera.dim.schemaregistry.testcontainers.env.envutils.config.Krb5ConfGenerator;
import com.cloudera.dim.schemaregistry.testcontainers.env.envutils.config.RegistryConfigGenerator;
import com.cloudera.dim.schemaregistry.testcontainers.env.envutils.config.ServletFilterConfigGenerator;
import com.cloudera.dim.schemaregistry.testcontainers.env.envutils.testcontainers.TestcontainersUtils;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.DbSetup;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.KerberosSetup;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.TlsSetup;
import com.cloudera.dim.schemaregistry.testcontainers.tests.testutils.SchemaRegistryHelper;
import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import lombok.Getter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.cloudera.dim.schemaregistry.testcontainers.env.envutils.testcontainers.TestcontainersUtils.*;


public abstract class TestcontainersTestsBase {

    private static Logger LOG = LoggerFactory.getLogger(TestcontainersTestsBase.class);

    @Getter
    private String testName;
    @Getter
    private Long testStartTime;
    @Getter
    private String schemaName;

    private final DbHandler dbHandler = new DbHandler();
    protected final SchemaRegistryHelper schemaRegistryHelper = new SchemaRegistryHelper();

    private static GenericContainer dbContainer;
    private static GenericContainer kerberosKdcContainer;
    private static GenericContainer srContainer;
    private static Network network;


    protected SchemaRegistryClient schemaRegistryClient;
    private LocalSchemaRegistryServerForTestcontainersTests testServer;

    private File temporaryFolder;


    protected abstract TestSetup getTestSetup(String tempFolderPath);


    @BeforeAll
    public void beforeAll() {
        LOG.info("Startup...");
        String className = this.getClass().getSimpleName();

        String tempTestFolderPath;
        try {
            temporaryFolder = Files.createTempDirectory("SrTestcontTempDir")
                    .toFile();
            temporaryFolder.deleteOnExit();
            File dirForClass = Paths.get(temporaryFolder.getAbsolutePath(), className).toFile();
            dirForClass.mkdirs();
            tempTestFolderPath = dirForClass.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Temp folder crated: " + tempTestFolderPath);

        TestSetup testSetup = getTestSetup(tempTestFolderPath);

        skipIfTestSetupCanNotBeRun(testSetup);

        createDockerNetwork();
        setupDbContainers(testSetup);

        if (testSetup.isKerberosEnabled()) {
            setSystemPropertiesForKerberosDebug();
            setupKerberosContainer(tempTestFolderPath);
            startKdcContainerAndUpdateKeytabPath(testSetup.getKerberosSetup(), tempTestFolderPath);

            String testKrb5ConfFile = generateKrb5ConfFile(
                    tempTestFolderPath,
                    getClass(),
                    false,
                    kerberosKdcContainer.getMappedPort(KDC_ADMIN_PORT),
                    kerberosKdcContainer.getMappedPort(KDC_PORT));

            testSetup.getKerberosSetup().setKrb5ConfFilePathForTest(testKrb5ConfFile);

            if (testSetup.isUseSrServerFromContainer()) {
                String containerizedSrServersKrb5ConfFilePath = generateKrb5ConfFile(
                        tempTestFolderPath,
                        getClass(),
                        true,
                        KDC_ADMIN_PORT,
                        KDC_PORT);
                testSetup.getKerberosSetup().setKrb5ConfFilePathForContainerizedSrServer(containerizedSrServersKrb5ConfFilePath);
            }
        }

        DbConnProps dbConnProps = startAttachedDbAndGetDbConnectionProperties(testSetup, dbContainer);

        dbConnProps = dbHandler.startDatabase(dbConnProps);

        RegistryYamlGenerator.TlsConfig tlsConfig = null;
        if (testSetup.isTlsEnabled()) {
            tlsConfig = createTlsConfig(testSetup.getTlsSetup(),
                    this.getClass(),
                    testSetup.getTempFolderPath());
        }

        int srServerPortIntoRegistryYaml;
        if (!testSetup.isUseSrServerFromContainer() && TestSetup.SR_USE_FIXED_PORT__DEV_ONLY) {
            srServerPortIntoRegistryYaml = SR_FIXED_PORT__DEV_ONLY;
        } else {
            srServerPortIntoRegistryYaml = testSetup.isUseSrServerFromContainer() ? SR_PORT : TestUtils.findFreePort();
        }

        String yamlFilePath = generateSrServerConfigYamlFile(
                testSetup,
                getClass(),
                dbConnProps,
                srServerPortIntoRegistryYaml,
                tlsConfig);


        if (testSetup.isUseSrServerFromContainer()) {
            setupSrServerContainer(
                    yamlFilePath,
                    tlsConfig,
                    testSetup.getKerberosSetup());

            srContainer.start();

            schemaRegistryClient = createSchemaRegistryClient(
                    testSetup,
                    getClass(),
                    srContainer.getFirstMappedPort());
        } else {
            testServer = new LocalSchemaRegistryServerForTestcontainersTests(dbConnProps, testSetup,
                    yamlFilePath, srServerPortIntoRegistryYaml, dbHandler);

            try {
                testServer.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            testServer.cleanupDb();

            LOG.info("TESTSERVER PORT:" + testServer.getSchemaRegistryPort());
            schemaRegistryClient = createSchemaRegistryClient(
                    testSetup,
                    getClass(),
                    testServer.getSchemaRegistryPort());
        }

        LOG.info("started");
    }


    @BeforeEach
    public void beforeEach(TestInfo method) {
        testName = method.getTestMethod().get().getName();
        testStartTime = System.currentTimeMillis();
        schemaName = String.format("%s-%s--ts:%s--uuid:%s",
                this.getClass().getSimpleName(), testName, testStartTime, UUID.randomUUID().toString());
        LOG.info("Started running test:" + testName);
    }

    @AfterEach
    public void tearDown() {
        LOG.info("Finished running test: " + testName);
    }

    @AfterAll
    public static void close() throws InterruptedException {
        List<GenericContainer> containersToStop = new ArrayList<>();
        tryStopContainer(dbContainer, containersToStop);
        tryStopContainer(kerberosKdcContainer, containersToStop);
        tryStopContainer(srContainer, containersToStop);

        awaitContainersStopped(containersToStop);
        if (network != null) {
            try {
                network.close();
            } catch (Throwable t) {
                LOG.error("Error when closing network", t);
            }
        }
    }


    private static SchemaRegistryClient createSchemaRegistryClient(
            TestSetup testSetup,
            Class classForResourcesAccess,
            int srServerPort) {

        Map<String, Serializable> config = new HashMap<>();

        String srServerHostname = "localhost";
        if (testSetup.isKerberosEnabled()) {
            srServerHostname = testSetup.getKerberosSetup().getSrServerPrincipalHostname();
        }

        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(),
                getBaseUrl(srServerHostname,
                        srServerPort,
                        testSetup.isTlsEnabled())
                        + "/api/v1");
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000);
        config.put("schema.registry.client.retry.policy", getRetryPolicyClientConfigEntries());

        if (testSetup.isOAuthEnabled()) {
            throw new UnsupportedOperationException("implementation could be copied from behavior tests");
        }

        if (testSetup.isKerberosEnabled()) {
            clearJaasConfSystemProperty();
            setKrb5ConfPathInSystemProperties(testSetup.getKerberosSetup().getKrb5ConfFilePathForTest());
            String principal = testSetup.getKerberosSetup().getTestPrincipalName();
            String keytabPath = testSetup.getKerberosSetup().getTestPrincipalKeytabPath();
            config.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required " +
                    "useKeyTab=true storeKey=true useTicketCache=false " +
                    "refreshKrb5Config=true " +
                    "keyTab=\"" + keytabPath + "\" " +
                    "principal=\"" + principal + "\";");
        }

        if (testSetup.isTlsEnabled()) {
            LOG.info("Configuring SR Client for TLS connection.");

            // put as SslMap is alternative to put "schema.registry.client.ssl.XY" entries one by one
            config.put("schema.registry.client.ssl", createSslMap(testSetup.isMTlsEnabled(),
                    classForResourcesAccess,
                    testSetup.getTempFolderPath()));
        }

        return new SchemaRegistryClient(config);
    }

    private static void setSystemPropertiesForKerberosDebug() {
        System.setProperty("sun.security.jgss.debug", "true");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("sun.security.spnego.debug", "true");
        System.setProperty("java.security.debug", "gssloginconfig,configfile,configparser,logincontext");
    }

    private static ImmutableMap<String, Serializable> getRetryPolicyClientConfigEntries() {
        HashMap<String, Serializable> retryPolicyParams = new HashMap(
                Stream.of(
                                new Object[]{"sleepTimeMs", 100},
                                new Object[]{"maxAttempts", 3},
                                new Object[]{"timeoutMs", 3000})
                        .collect(Collectors.toMap((data) -> {
                            return (String) data[0];
                        }, (data) -> {
                            return (Integer) data[1];
                        })));
        ImmutableMap<String, Serializable> retryPolicy = ImmutableMap.of(
                "className", "com.hortonworks.registries.schemaregistry.retry.policy.ExponentialBackoffPolicy",
                "config", retryPolicyParams);
        return retryPolicy;
    }

    private static void clearJaasConfSystemProperty() {
        System.clearProperty("java.security.auth.login.config");
    }

    private static void setKrb5ConfPathInSystemProperties(String krb5ConfFilePath) {
        String oldConfPath = System.getProperty("java.security.krb5.conf");
        System.setProperty("java.security.krb5.conf", krb5ConfFilePath);
        // refreshKrb5Config=true must be set in the JAAS config, otherwise the new path has no effect
        LOG.info("JVM property about krb5.conf location changed from " +
                oldConfPath + " to " + krb5ConfFilePath);
    }

    private static String getBaseUrl(String hostname, int port, boolean isHttpsScheme) {
        if (isHttpsScheme) {
            return String.format("https://%s:%d", hostname, port);
        }
        return String.format("http://%s:%d", hostname, port);
    }


    private static HashMap<String, String> createSslMap(boolean mTls, Class classForResourceAccess, String tempFolderPath) {
        HashMap<String, String> sslMap = new HashMap();
        sslMap.put("protocol", "SSL");
        sslMap.put("trustStoreType", "JKS");
        sslMap.put("trustStorePath", TestUtils.getResourceAsTempFile(classForResourceAccess, "/tls/truststore.jks",
                Paths.get(tempFolderPath, "client_truststore.jks").toString()));
        sslMap.put("trustStorePassword", "password");
        if (mTls) {
            sslMap.put("keyStoreType", "JKS");
            sslMap.put("keyStorePath", TestUtils.getResourceAsTempFile(classForResourceAccess, "/tls/keystore.jks",
                    Paths.get(tempFolderPath, "client_keystore.jks").toString()));
            sslMap.put("keyStorePassword", "password");
        }
        return sslMap;
    }

    private static void tryStopContainer(GenericContainer<?> container, List<GenericContainer> stopContainersList) {
        if (container == null) {
            return;
        }
        if (!container.isRunning()) {
            return;
        }
        LOG.info("Stopping container from image " + container.getDockerImageName());
        container.stop();
        stopContainersList.add(container);
    }

    private static void awaitContainersStopped(List<GenericContainer> containers) {
        LOG.info("Wait for " + containers.size() + " containers to stop...");
        if (containers.size() < 1) {
            return;
        }
        List<GenericContainer> stillRunning = new ArrayList<>(containers);
        GenericContainer cont = stillRunning.get(0);
        long startTime = System.currentTimeMillis();
        long maxTimeMs = 10 * 1000;
        while (!stillRunning.isEmpty() && (System.currentTimeMillis() - startTime) < maxTimeMs) {
            while (cont.isRunning() && (System.currentTimeMillis() - startTime) < maxTimeMs) {
                LOG.debug("Containers still stopping...");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
            stillRunning.remove(cont);
            if (stillRunning.size() > 0) {
                cont = stillRunning.get(0);
            } else {
                break;
            }
        }
    }


    private static String generateKrb5ConfFile(String tempTestFolderPath,
                                               Class classForResourceAccess,
                                               boolean generateForContainerizedSrServer,
                                               int kdcContainerAdminPort,
                                               int kdcContainerTgsPort) {

        String krb5ConfTxt = Krb5ConfGenerator.generateKrb5ConfContent(
                generateForContainerizedSrServer,
                classForResourceAccess,
                "/template",
                kdcContainerAdminPort,
                kdcContainerTgsPort
        );

        String filename;
        if (generateForContainerizedSrServer) {
            filename = "containerizedSrServer_krb5.conf";
        } else {
            filename = "test_krb5.conf";
        }

        File krb5Conf = TestUtils.writeFileToTempDir(filename, "", krb5ConfTxt,
                tempTestFolderPath);

        LOG.debug("krb5.conf file generated at {}", krb5Conf.getAbsolutePath());

        return krb5Conf.getAbsolutePath();
    }

    private static String generateSrServerConfigYamlFile(TestSetup testSetup, Class classForResourcesAccess,
                                                         DbConnProps dbProperties,
                                                         int schemaRegistryPort,
                                                         RegistryYamlGenerator.TlsConfig tlsConfig) {

        List<ServletFilterConfiguration> optionalServletFilters = ServletFilterConfigGenerator.prepareServletFilters(
                testSetup.isTlsEnabled(),
                testSetup.isOAuthEnabled(), classForResourcesAccess,
                testSetup.getKerberosSetup(),
                testSetup.isMTlsEnabled(),
                testSetup.getAdditionalFilters());

        RegistryConfiguration config = RegistryConfigGenerator.prepareConfig(dbProperties,
                testSetup.getAtlasSetup(),
                optionalServletFilters,
                testSetup.getDefaultAvroSchemaCompatibility(),
                testSetup.getDefaultJsonSchemaCompatibility(),
                testSetup.isUseSrServerFromContainer());

        String registryYamlTxt = RegistryYamlGenerator.generateRegistryYaml(
                config,
                testSetup.isTlsEnabled() ? "https" : "http",
                schemaRegistryPort,
                tlsConfig,
                classForResourcesAccess,
                "/template");

        File registryYaml = TestUtils.writeFileToTempDir("registry", ".yaml", registryYamlTxt,
                testSetup.getTempFolderPath());
        LOG.debug("registry.yaml file generated at {}", registryYaml.getAbsolutePath());

        return registryYaml.getAbsolutePath();
    }

    private static void skipIfTestSetupCanNotBeRun(TestSetup testSetup) {
        if (testSetup.isUseSrServerFromContainer()
                && testSetup.getDbSetup().getUsedDbType() == DbType.H2) {
            String msg = "Test skipped because when SR server running in container it can not use H2 DB.";
            LOG.warn(msg);
            throw new TestAbortedException(msg);
        }
    }


    private static void createDockerNetwork() {
        network = Network.builder()
                .enableIpv6(false)
                .build();
        String networkId = network.getId();
        LOG.info("New docker network will be created with ID:" + networkId);
    }

    private static void setupDbContainers(TestSetup testSetup) {
        switch (testSetup.getDbSetup().getUsedDbType()) {
            case MYSQL5:
            case MYSQL8:
                dbContainer = TestcontainersUtils.prepareMySqlContainer(network, testSetup.getDbSetup());
                break;
            case POSTGRES:
                dbContainer = TestcontainersUtils.preparePostgresContainer(network, testSetup.getDbSetup());
                break;
            default:
                LOG.info("Use H2 memory database...");
                break;
        }
    }

    private static void setupKerberosContainer(String tempTestFolderPath) {
        kerberosKdcContainer = TestcontainersUtils.prepareKdcContainer(tempTestFolderPath, network);
    }

    private static void setupSrServerContainer(String generatedSrYamlPath,
                                               RegistryYamlGenerator.TlsConfig tlsConfig,
                                               KerberosSetup kerberosSetup) {
        String serverKeystorePath = null;
        String serverTruststorePath = null;
        String krb5ConfPath = null;
        String serverPrincipalKeytabPath = null;

        boolean isHttps = false;
        boolean isKerberized = false;

        if (tlsConfig != null) {
            serverKeystorePath = tlsConfig.getKeyStorePath();
            serverTruststorePath = tlsConfig.getTrustStorePath();
            isHttps = true;
        }
        if (kerberosSetup != null) {
            isKerberized = true;
            serverPrincipalKeytabPath = kerberosSetup.getSrServerPrincipalKeytabPath();
            krb5ConfPath = kerberosSetup.getKrb5ConfFilePathForContainerizedSrServer();
        }

        GenericContainer[] dependsOnContainers = new GenericContainer[0];
        if (dbContainer != null) {
            dependsOnContainers =
                    new GenericContainer[1];
            dependsOnContainers[0] = dbContainer;
        }

        srContainer = TestcontainersUtils.prepareSRContainer(generatedSrYamlPath,
                isHttps,
                serverKeystorePath,
                serverTruststorePath,
                isKerberized,
                krb5ConfPath,
                serverPrincipalKeytabPath,
                network, dependsOnContainers);

    }

    private static DbConnProps startAttachedDbAndGetDbConnectionProperties(TestSetup testSetup, GenericContainer dbContainer) {

        if (dbContainer != null) {
            dbContainer.start();
        }
        DbSetup dbSetup = testSetup.getDbSetup();
        DbConnProps dbConnProps;
        if (dbSetup.getUsedDbType() != DbType.H2) {
            dbConnProps = new DbConnProps(
                    dbSetup.getUsedDbType(),
                    dbSetup.getDbUserName(),
                    dbSetup.getDbUserPass(),
                    dbContainer.getFirstMappedPort(),
                    dbContainer.getHost(),
                    dbSetup.getDbName());
        } else {
            dbConnProps = new DbConnProps(
                    DbType.H2,
                    "", "",
                    0, "", "");

        }

        return dbConnProps;
    }

    private static void startKdcContainerAndUpdateKeytabPath(KerberosSetup kerberosSetup, String tempTestFolderPath) {
        kerberosKdcContainer.start();
        TestcontainersUtils.awaitKdcContainerIsUp(kerberosKdcContainer);

        TestcontainersUtils.addPrincipalToKDC(kerberosKdcContainer,
                kerberosSetup.getSrServerPrincipalName(),
                kerberosSetup.getSrServerPrincipalPassword());
        TestcontainersUtils.getKeytabToMountedDir(kerberosKdcContainer,
                kerberosSetup.getSrServerPrincipalName(),
                kerberosSetup.getSrServerPrincipalKeytabFilename());
        TestcontainersUtils.execInTestcontainer(kerberosKdcContainer,
                TestcontainersUtils.chmodKeytabFile(MOUNTED_DIR_PATH_IN_KDC_CONTAINER + "/" +
                        kerberosSetup.getSrServerPrincipalKeytabFilename()));

        String srServerPrincipalKeytabFullPath = tempTestFolderPath + "/" +
                kerberosSetup.getSrServerPrincipalKeytabFilename();
        LOG.info("SR_SERVER_PRINCIPAL keytab can be found at: " + srServerPrincipalKeytabFullPath);
        kerberosSetup.setSrServerPrincipalKeytabPath(srServerPrincipalKeytabFullPath);

        TestcontainersUtils.addPrincipalToKDC(kerberosKdcContainer,
                kerberosSetup.getTestPrincipalName(),
                kerberosSetup.getTestPrincipalPassword());
        TestcontainersUtils.getKeytabToMountedDir(kerberosKdcContainer,
                kerberosSetup.getTestPrincipalName(),
                kerberosSetup.getTestPrincipalKeytabFilename());
        TestcontainersUtils.execInTestcontainer(kerberosKdcContainer,
                TestcontainersUtils.chmodKeytabFile(MOUNTED_DIR_PATH_IN_KDC_CONTAINER + "/" +
                        kerberosSetup.getTestPrincipalKeytabFilename()));
        String testPrincipalKeytabFullPath = tempTestFolderPath + "/" +
                kerberosSetup.getTestPrincipalKeytabFilename();
        LOG.info("TEST_PRINCIPAL keytab can be found at: " + testPrincipalKeytabFullPath);

        kerberosSetup.setTestPrincipalKeytabPath(testPrincipalKeytabFullPath);
    }


    private static RegistryYamlGenerator.TlsConfig createTlsConfig(TlsSetup tlsSetup,
                                                                   Class classForResourceAccess, String tempFolderPath) {

        boolean clientAuthEnabled = tlsSetup.getClientAuthRequired();

        RegistryYamlGenerator.TlsConfig ret = new RegistryYamlGenerator.TlsConfig(
                clientAuthEnabled,
                TestUtils.getResourceAsTempFile(classForResourceAccess, "/tls/keystore.jks",
                        Paths.get(tempFolderPath, "server_keystore.jks").toString()),
                "password",
                "selfsigned",
                false,
                TestUtils.getResourceAsTempFile(classForResourceAccess, "/tls/truststore.jks",
                        Paths.get(tempFolderPath, "server_truststore.jks").toString()),
                "password"
        );

        return ret;
    }
}
