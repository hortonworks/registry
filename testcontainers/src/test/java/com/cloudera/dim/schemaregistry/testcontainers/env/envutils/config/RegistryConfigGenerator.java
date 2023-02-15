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

package com.cloudera.dim.schemaregistry.testcontainers.env.envutils.config;

import com.cloudera.dim.schemaregistry.TestUtils;
import com.cloudera.dim.schemaregistry.atlas.TestAtlasPluginProvider;
import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbConnProps;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.AtlasSetup;
import com.hortonworks.registries.common.AtlasConfiguration;
import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.FileStorageProperties;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.common.util.LocalFileSystemStorage;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.storage.StorageProviderProperties;
import com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RegistryConfigGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryConfigGenerator.class);
    private static final int ATLAS_CONFIG_WAIT_BETWEEN_AUDIT_PROCESSING = 100;

    private RegistryConfigGenerator() {

    }

    public static RegistryConfiguration prepareConfig(DbConnProps dbProps, AtlasSetup atlasSetup,
                                                      List<ServletFilterConfiguration> servletFilters,
                                                      SchemaCompatibility defaultAvroCompatibility,
                                                      SchemaCompatibility defaultJsonCompatibility,
                                                      boolean isSrServerRunningInContainer) {
        RegistryConfiguration configuration = new RegistryConfiguration();

        addAtlasConfigEntries(configuration, atlasSetup);

        addStorageProviderConfigEntries(configuration, dbProps, isSrServerRunningInContainer);

        addFileStorageConfigEntries(configuration);

        addCompatibilityConfigEntries(configuration, defaultAvroCompatibility, defaultJsonCompatibility);

        ServletFilterConfigGenerator.addOptionalServletFilterConfigEntries(configuration, servletFilters);

        ServletFilterConfigGenerator.addRewriteServletFilterConfig(configuration);

        return configuration;
    }


    private static void addAtlasConfigEntries(RegistryConfiguration configuration,
                                              AtlasSetup atlasSetup) {
        if (configuration.getAtlasConfiguration() == null) {
            AtlasConfiguration atlasConfiguration = new AtlasConfiguration();
            AtlasConfiguration.BasicAuth basicAuth = new AtlasConfiguration.BasicAuth();
            basicAuth.setUsername("kafka");
            basicAuth.setPassword("cloudera");
            atlasConfiguration.setBasicAuth(basicAuth);
            atlasConfiguration.setWaitBetweenAuditProcessing(ATLAS_CONFIG_WAIT_BETWEEN_AUDIT_PROCESSING);

            if (atlasSetup != null) {
                atlasConfiguration.setEnabled(true);
                atlasConfiguration.setAtlasUrls(Collections.singletonList("http://localhost:" +
                        atlasSetup.getAtlasPort()));
                atlasConfiguration.setCustomPluginProvider(TestAtlasPluginProvider.class.getName());

                // $root/behavior-tests/build/classes/java/test/
                File atlasJarsDir = null;
                for (String subdir : Arrays.asList("../../../atlasJars", "../../atlasJars", "../atlasJars")) {
                    File f = new File(atlasSetup.getResourcesClassForAtlasJarSearch().getResource("/").getFile(), subdir);
                    if (f.exists() && f.isDirectory()) {
                        atlasJarsDir = f;
                        break;
                    }
                }

                if (atlasJarsDir == null) {
                    atlasJarsDir = TestUtils.findSubdir(new File(System.getProperty("user.dir")), "atlasJars");
                }

                if (atlasJarsDir == null) {
                    LOG.error("The test was unable to load Atlas JAR files from the classpath. " +
                            "If you are running from an IDE then please examine where the tests are being run from.");
                    throw new RuntimeException();
                } else {
                    String customClasspath = atlasJarsDir.toPath().normalize().toAbsolutePath().toString();
                    if (File.separatorChar == '\\') {
                        customClasspath = customClasspath.replaceAll("\\\\", "/");
                    }
                    atlasConfiguration.setCustomClasspath(customClasspath);
                }
            }

            configuration.setAtlasConfiguration(atlasConfiguration);
        }
    }

    private static void addStorageProviderConfigEntries(RegistryConfiguration configuration, DbConnProps dbProps,
                                                        boolean isSrServerRunningInContainer) {
        if (configuration.getStorageProviderConfiguration() == null) {
            StorageProviderConfiguration storageConfig = new StorageProviderConfiguration();
            StorageProviderProperties properties = new StorageProviderProperties();
            properties.setDbtype(dbProps.getDbType().getDbType().getValue());
            properties.setQueryTimeoutInSecs(30);
            properties.setProperties(dbProps.getDbPropertiesForRegistryYaml(isSrServerRunningInContainer));

            storageConfig.setProviderClass(JdbcStorageManager.class.getName());
            storageConfig.setProperties(properties);
            configuration.setStorageProviderConfiguration(storageConfig);
        }
    }

    private static void addFileStorageConfigEntries(RegistryConfiguration configuration) {
        if (configuration.getFileStorageConfiguration() == null) {
            FileStorageConfiguration fileConfig = new FileStorageConfiguration();
            fileConfig.setClassName(LocalFileSystemStorage.class.getName());
            FileStorageProperties props = new FileStorageProperties();
            String uploadtmp = null;
            try {
                uploadtmp = Files.createTempDirectory("uploadtmp").toFile().getAbsolutePath();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (File.separatorChar == '\\') {
                props.setDirectory(uploadtmp.replaceAll("\\\\", "/"));
            } else {
                props.setDirectory(uploadtmp);
            }
            fileConfig.setProperties(props);
            configuration.setFileStorageConfiguration(fileConfig);
        }
    }

    private static void addCompatibilityConfigEntries(RegistryConfiguration configuration,
                                                      SchemaCompatibility defaultAvroCompatibility,
                                                      SchemaCompatibility defaultJsonCompatibility) {
        if (defaultAvroCompatibility != null) {
            configuration.getCompatibility().setAvroCompatibility(defaultAvroCompatibility.name());
        }
        if (defaultJsonCompatibility != null) {
            configuration.getCompatibility().setJsonCompatibility(defaultJsonCompatibility.name());
        }
    }


}
