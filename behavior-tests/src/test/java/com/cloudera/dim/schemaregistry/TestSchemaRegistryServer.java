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
package com.cloudera.dim.schemaregistry;

import com.cloudera.dim.registry.oauth2.JwtKeyStoreType;
import com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler;
import com.cloudera.dim.registry.oauth2.OAuth2Config;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hortonworks.registries.auth.server.AuthenticationFilter;
import com.hortonworks.registries.common.AtlasConfiguration;
import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.FileStorageProperties;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.common.util.LocalFileSystemStorage;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import com.hortonworks.registries.schemaregistry.webservice.RewriteUriFilter;
import com.hortonworks.registries.storage.DbProperties;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.storage.StorageProviderProperties;
import com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.atlas.plugin.classloader.AtlasCustomPathClassLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.h2.Driver;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestSchemaRegistryServer extends AbstractTestServer {

    private static final Logger LOG = LoggerFactory.getLogger(TestSchemaRegistryServer.class);

    // we'll simulate a mysql database with H2
    private static final String DB_TYPE = "mysql";

    private static final String CONNECTION_URL_TEMPLATE = "jdbc:h2:%s:test;MODE=MYSQL;DATABASE_TO_UPPER=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;INIT=%s";
    private static final int ATLAS_CONFIG_WAIT_BETWEEN_AUDIT_PROCESSING = 100;

    private ExecutorService threadPool;
    private org.h2.tools.Server h2Server;
    private int h2Port;
    private int schemaRegistryPort;
    private Flyway flyway;
    private LocalSchemaRegistryServer localSchemaRegistry;
    private boolean atlasEnabled = false;
    private int atlasPort = -1;
    private DataSource dataSource;
    private List<ServletFilterConfiguration> additionalFilters;
    private boolean oauth2Enabled = false;
    private String defaultAvroCompatibility = SchemaCompatibility.DEFAULT_COMPATIBILITY.name();

    private volatile static TestSchemaRegistryServer instance;

    public static TestSchemaRegistryServer getInstance() {
        TestSchemaRegistryServer localRef = instance;
        if (localRef == null) {
            synchronized (TestSchemaRegistryServer.class) {
                if (localRef == null) {
                    instance = localRef = new TestSchemaRegistryServer();
                }
            }
        }
        return localRef;
    }

    @Override
    public void start() throws Exception {
        boolean alreadyStarted = started.getAndSet(true);
        if (alreadyStarted) {
            return;
        }

        if (threadPool == null) {
            threadPool = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("sr-%d").setDaemon(true).build());
        }

        DbProperties dbProperties = startDatabase();
        this.flyway = populateDatabase(dbProperties);

        List<ServletFilterConfiguration> filters = prepareServletFilters(oauth2Enabled, additionalFilters);

        // now we can start Schema Registry and have it connect to our H2 database
        RegistryConfiguration config = prepareConfig(dbProperties, atlasEnabled, atlasPort, filters, defaultAvroCompatibility);

        this.schemaRegistryPort = findFreePort();
        String registryYamlTxt = configGenerator.generateRegistryYaml(config, schemaRegistryPort);

        File registryYaml = writeFile("registry", ".yaml", registryYamlTxt);
        LOG.debug("registry.yaml file generated at {}", registryYaml.getAbsolutePath());

        this.localSchemaRegistry = new LocalSchemaRegistryServer(registryYaml.getAbsolutePath());
        Future<Boolean> srStarted = threadPool.submit(() -> {
            try {
                localSchemaRegistry.start();
                return true;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        srStarted.get(1, TimeUnit.MINUTES);
        this.running.set(true);
    }

    @Override
    public void stop() throws Exception {
        try {
            localSchemaRegistry.stop();
        } catch (Exception ex) { }
        try {
            h2Server.shutdown();
            h2Server.stop();
        } catch (Exception ex) { }
        try {
            threadPool.shutdown();
            threadPool = null;
        } catch (Exception ex) { }
        super.stop();
    }

    /** Clean the H2 database. */
    public void cleanupDb() {
        LOG.info("Cleaning up the database ...");
        flyway.clean();
        flyway.migrate();
    }

    public Server getH2Server() {
        return h2Server;
    }

    /** Get the port where the h2 dabatase is running. */
    public int getH2Port() {
        return h2Port;
    }

    /** Get the port where schema registry is running. */
    public int getPort() {
        return schemaRegistryPort;
    }

    /** Default properties to connect to in-memory H2. */
    private DbProperties getH2DbProperties() {
        DbProperties props = new DbProperties();
        props.setDataSourceClassName(JdbcDataSource.class.getName());
        props.setDataSourceUser("sa");
        props.setDataSourcePassword("");
        props.setDataSourceUrl(String.format(CONNECTION_URL_TEMPLATE, "mem", ""));
        return props;
    }

    /** Open connection pool to the in-memory H2 database. */
    public DataSource startInMemoryH2() {
        DbProperties h2dbProps = getH2DbProperties();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(Driver.class.getName());
        hikariConfig.setJdbcUrl(h2dbProps.getDataSourceUrl());
        hikariConfig.setUsername(h2dbProps.getDataSourceUser());
        hikariConfig.setPassword(h2dbProps.getDataSourcePassword());
        hikariConfig.setAutoCommit(true);
        hikariConfig.setConnectionInitSql("");
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setMaximumPoolSize(30);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        return new HikariDataSource(hikariConfig);
    }

    /** Schema Registry runs as an external process, so we need to provide a TCP port where it can connect to H2. */
    private void openH2TcpPort() throws SQLException {
        h2Server = org.h2.tools.Server.createTcpServer("-tcp", "-tcpAllowOthers");
        h2Server.start();
        h2Port = h2Server.getPort();
    }

    private DbProperties startDatabase() throws SQLException {
        LOG.debug("Setting up H2 database ...");

        // start in-memory H2 database
        dataSource = startInMemoryH2();

        // open TCP port so external processes can access it
        openH2TcpPort();

        // get URL to access the db externally
        DbProperties dbConnProps = getH2DbProperties();
        dbConnProps.setDataSourceUrl(String.format(CONNECTION_URL_TEMPLATE, "tcp://localhost:" + h2Port + "/mem", ""));

        return dbConnProps;
    }

    private Flyway populateDatabase(DbProperties dbConnProps) throws IOException {
        Flyway flyway = getFlyway(dbConnProps, preprocessMigrations().getAbsolutePath());
        flyway.migrate();
        return flyway;
    }

    private List<ServletFilterConfiguration> prepareServletFilters(boolean oauth2Enabled,
                                           List<ServletFilterConfiguration> additionalFilters) throws IOException {
        List<ServletFilterConfiguration> servletFilters = additionalFilters;
        if (servletFilters == null) {
            servletFilters = new ArrayList<>();
        }

        if (oauth2Enabled) {
            ServletFilterConfiguration filterConfig = new ServletFilterConfiguration();
            servletFilters.add(filterConfig);
            filterConfig.setClassName(AuthenticationFilter.class.getName());
            Map<String, String> params = new LinkedHashMap<>();;
            filterConfig.setParams(params);
            params.put("type", OAuth2AuthenticationHandler.class.getName());
            params.put(OAuth2Config.KEY_STORE_TYPE, JwtKeyStoreType.PROPERTY.getValue());
            params.put(OAuth2Config.KEY_ALGORITHM, "RS256");
            params.put(OAuth2Config.PUBLIC_KEY_PROPERTY,
                    IOUtils.toString(getClass().getResource("/template/test_rsa.pub"), StandardCharsets.UTF_8)
                            .replaceAll("\n", ""));
        }

        if (!servletFilters.isEmpty()) {
            LOG.info("{} servlet filters: {}", servletFilters.size(), servletFilters.stream()
                    .map(ServletFilterConfiguration::getClassName)
                    .collect(Collectors.joining(", ")));
        }

        return servletFilters;
    }

    /** The DDL files under bootstrap need to be sanitized before we can pass them to H2. */
    private File preprocessMigrations() throws IOException {
        File tmpDir = Files.createTempDirectory("srtest").toFile();

        File bootstrapDir = getPathToBootstrap(DB_TYPE);
        for (File file : bootstrapDir.listFiles()) {
            if (file.getName().startsWith("v006")) {
                continue;
            }
            List<String> lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
            File outFile = new File(tmpDir, file.getName());

            try (PrintWriter out = new PrintWriter(new FileWriter(outFile))) {
                boolean ignore = false;
                for (String line : lines) {
                    // ignore commented lines or lines which contain procedure operations
                    if (line.startsWith("--") || line.contains("Cloudera") || line.toUpperCase().contains("CALL")
                            || line.toUpperCase().contains("DROP PROCEDURE")) {
                        continue;
                    }
                    // H2 has no support for MySql functions, but luckily we can ignore them
                    if (!ignore && line.toLowerCase().contains("delimiter")) {
                        ignore = true;
                    } else if (ignore && line.toLowerCase().contains("delimiter")) {
                        ignore = false;
                    } else if (!ignore) {
                        out.println(line);
                    }
                }
            } catch (Exception ex) {
                LOG.error("Failure while writing file {}", file.getName(), ex);
            }
            outFile.deleteOnExit();

            LOG.debug("Wrote preprocessed DDL file {}", outFile.getAbsolutePath());
        }

        return tmpDir;
    }

    /** Usually bootstrap dir should be under the root, but let's try to play safer and look at a few other places too. */
    @Nonnull
    private File getPathToBootstrap(String dbType) throws FileNotFoundException {
        // we want to find the path to /bootstrap/sql/mysql

        File[] files = {
            new File("bootstrap/sql/" + dbType),
            new File(System.getProperty("user.dir"), "bootstrap/sql/" + dbType),
            new File(System.getProperty("user.home"), "bootstrap/sql/" + dbType),
            new File("../bootstrap/sql/" + dbType),
            new File("../../bootstrap/sql/" + dbType),
            new File("../../../bootstrap/sql/" + dbType)
        };

        for (File file : files) {
            try {
                if (file.exists() && file.isDirectory()) {
                    LOG.debug("Bootstrap directory: {}", file.getAbsolutePath());
                    return file;
                }
            } catch (Throwable ex) {
                LOG.trace("Unexpected error for " + file, ex);
            }
        }

        throw new FileNotFoundException("Could not find bootstrap directory near " + new File(".").getAbsolutePath());
    }

    /** Use Flyway to create the data structure in H2. */
    private Flyway getFlyway(DbProperties conf, String location) {
        Flyway flyway = new Flyway();

        flyway.setEncoding("UTF-8");
        flyway.setTable("SCRIPT_CHANGE_LOG");
        flyway.setValidateOnMigrate(true);
        flyway.setOutOfOrder(false);
        flyway.setBaselineOnMigrate(true);
        flyway.setBaselineVersion(MigrationVersion.fromVersion("000"));
        flyway.setCleanOnValidationError(false);
        flyway.setLocations("filesystem:" + location);
        flyway.setSqlMigrationPrefix("v");
        flyway.setDataSource(conf.getDataSourceUrl(), conf.getDataSourceUser(), conf.getDataSourcePassword());

        return flyway;
    }

    /** Prepare a configuration which will be passed to Schema Registry. */
    public RegistryConfiguration prepareConfig(DbProperties h2DbProps, boolean atlasEnabled, int atlasPort,
                                               List<ServletFilterConfiguration> servletFilters,
                                               String defaultAvroCompatibility) throws IOException {
        RegistryConfiguration configuration = new RegistryConfiguration();
        if (configuration.getAtlasConfiguration() == null) {
            AtlasConfiguration atlasConfiguration = new AtlasConfiguration();
            AtlasConfiguration.BasicAuth basicAuth = new AtlasConfiguration.BasicAuth();
            basicAuth.setUsername("kafka");
            basicAuth.setPassword("cloudera");
            atlasConfiguration.setBasicAuth(basicAuth);
            atlasConfiguration.setWaitBetweenAuditProcessing(ATLAS_CONFIG_WAIT_BETWEEN_AUDIT_PROCESSING);

            if (atlasEnabled) {
                atlasConfiguration.setEnabled(true);
                atlasConfiguration.setAtlasUrls(Collections.singletonList("http://localhost:" + atlasPort));
                atlasConfiguration.setCustomClasspathLoader(AtlasCustomPathClassLoader.class.getName());

                // $root/behavior-tests/build/classes/java/test/
                File atlasJarsDir = null;
                for (String subdir : Arrays.asList("../../../atlasJars", "../../atlasJars", "../atlasJars")) {
                    File f = new File(getClass().getResource("/").getFile(), subdir);
                    if (f.exists() && f.isDirectory()) {
                        atlasJarsDir = f;
                        break;
                    }
                }

                String customClasspath = atlasJarsDir.toPath().normalize().toAbsolutePath().toString();
                if (File.separatorChar == '\\') {
                    customClasspath = customClasspath.replaceAll("\\\\", "/");
                }
                atlasConfiguration.setCustomClasspath(customClasspath);
            }

            configuration.setAtlasConfiguration(atlasConfiguration);
        }
        if (configuration.getStorageProviderConfiguration() == null) {
            StorageProviderConfiguration storageConfig = new StorageProviderConfiguration();
            StorageProviderProperties properties = new StorageProviderProperties();
            properties.setDbtype(DB_TYPE);
            properties.setQueryTimeoutInSecs(30);
            properties.setProperties(h2DbProps);

            storageConfig.setProviderClass(JdbcStorageManager.class.getName());
            storageConfig.setProperties(properties);
            configuration.setStorageProviderConfiguration(storageConfig);
        }
        if (configuration.getFileStorageConfiguration() == null) {
            FileStorageConfiguration fileConfig = new FileStorageConfiguration();
            fileConfig.setClassName(LocalFileSystemStorage.class.getName());
            FileStorageProperties props = new FileStorageProperties();
            String uploadtmp = Files.createTempDirectory("uploadtmp").toFile().getAbsolutePath();
            if (File.separatorChar == '\\') {
                props.setDirectory(uploadtmp.replaceAll("\\\\", "/"));
            } else {
                props.setDirectory(uploadtmp);
            }
            fileConfig.setProperties(props);
            configuration.setFileStorageConfiguration(fileConfig);
        }

        if (configuration.getServletFilters() == null) {
            configuration.setServletFilters(new ArrayList<>());
        }
        if (servletFilters != null) {
            configuration.getServletFilters().addAll(servletFilters);
        }
        if (defaultAvroCompatibility != null) {
            configuration.getCompatibility().setAvroCompatibility(defaultAvroCompatibility);
        }

        ServletFilterConfiguration rewriteFilterConf = new ServletFilterConfiguration();
        configuration.getServletFilters().add(rewriteFilterConf);
        rewriteFilterConf.setClassName(RewriteUriFilter.class.getName());
        Map<String, String> params = new LinkedHashMap<>();;
        rewriteFilterConf.setParams(params);
        params.put("forwardPaths", "/api/v1/confluent,/subjects/*,/schemas/ids/*");
        params.put("redirectPaths", "/ui/,/");

        return configuration;
    }

    public boolean isAtlasEnabled() {
        return atlasEnabled;
    }

    public void setAtlasEnabled(boolean atlasEnabled) {
        this.atlasEnabled = atlasEnabled;
    }

    public int getAtlasPort() {
        return atlasPort;
    }

    public void setAtlasPort(int atlasPort) {
        this.atlasPort = atlasPort;
    }

    public int countFailedAtlasEvents() throws SQLException {
        try (
                PreparedStatement statement = dataSource
                        .getConnection()
                        .prepareStatement("select count(*) from atlas_events where processed=? and failed=?")
        ) {
            statement.setBoolean(1, false);
            statement.setBoolean(2, true);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.first();
                int failed = resultSet.getInt(1);
                LOG.debug("there are {} failed atlas events", failed);
                return failed;
            }
        }
    }

    public boolean isOAuth2Enabled() {
        return oauth2Enabled;
    }

    public void setOAuth2Enabled(boolean oauth2Enabled) {
        this.oauth2Enabled = oauth2Enabled;
    }

    public void setAdditionalFilters(List<ServletFilterConfiguration> additionalFilters) {
        this.additionalFilters = additionalFilters;
    }

    public String getDefaultAvroCompatibility() {
        return defaultAvroCompatibility;
    }

    public void setDefaultAvroCompatibility(String defaultAvroCompatibility) {
        this.defaultAvroCompatibility = defaultAvroCompatibility;
    }
}
