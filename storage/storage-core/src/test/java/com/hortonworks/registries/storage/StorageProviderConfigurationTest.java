/**
 * Copyright 2016-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.configuration.ConfigurationParsingException;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.util.Resources;
import org.junit.jupiter.api.Test;

import javax.validation.Validator;
import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StorageProviderConfigurationTest {
    private final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private final Validator validator = Validators.newValidator();
    private final YamlConfigurationFactory<StorageProviderConfiguration> factory =
            new YamlConfigurationFactory<>(StorageProviderConfiguration.class, validator, objectMapper, "");


    @Test
    public void testReadCorrectYaml() throws Exception {
        final File yml = new File(Resources.getResource("storageconfigtest.yaml").toURI());
        final StorageProviderConfiguration wid = factory.build(yml);
        String providerClass = wid.getProviderClass();
        StorageProviderProperties storageProviderProperties = wid.getProperties();
        String dbType = storageProviderProperties.getDbtype();
        int queryTimeoutInSecs = storageProviderProperties.getQueryTimeoutInSecs();
        DbProperties dbProperties = storageProviderProperties.getProperties();
        String dataSourceClassName = dbProperties.getDataSourceClassName();
        String dataSourceUrl = dbProperties.getDataSourceUrl();
        String dataSourceUser = dbProperties.getDataSourceUser();
        String dataSourcePassword = dbProperties.getDataSourcePassword();
        ConnectionProperties connectionProperties = dbProperties.getConnectionProperties();
        String oracleNetSslVersion = connectionProperties.getOracleNetSslVersion();
        Boolean oracleNetSslServerDnMatch = connectionProperties.getOracleNetSslServerDnMatch();
        String trustStore = connectionProperties.getTrustStore();
        String trustStoreType = connectionProperties.getTrustStoreType();
        String keyStore = connectionProperties.getKeyStore();
        String keyStoreType = connectionProperties.getKeyStoreType();
        assertEquals("com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager", providerClass);
        assertEquals("mysql", dbType);
        assertEquals(30, queryTimeoutInSecs);
        assertEquals("com.mysql.jdbc.jdbc2.optional.MysqlDataSource", dataSourceClassName);
        assertEquals("jdbc:mysql://localhost/schema_registry", dataSourceUrl);
        assertEquals("registry_user", dataSourceUser);
        assertEquals("registry_password", dataSourcePassword);
        assertEquals("1", oracleNetSslVersion);
        assertEquals(true, oracleNetSslServerDnMatch);
        assertEquals("~/Oracle/Wallets/TrustStore/cwallet.sso", trustStore);
        assertEquals("SSO", trustStoreType);
        assertEquals("~/Oracle/Wallets/Client/cwallet.sso", keyStore);
        assertEquals("SSO", keyStoreType);
    }

    @Test
    public void testReadNullYaml() throws Exception {
        final File yml = new File(Resources.getResource("storageconfigbadtest.yaml").toURI());
        final StorageProviderConfiguration wid = factory.build(yml);
        String providerClass = wid.getProviderClass();
        StorageProviderProperties storageProviderProperties = wid.getProperties();
        String dbType = storageProviderProperties.getDbtype();
        int queryTimeoutInSecs = storageProviderProperties.getQueryTimeoutInSecs();
        DbProperties dbProperties = storageProviderProperties.getProperties();
        String dataSourceClassName = dbProperties.getDataSourceClassName();
        String dataSourceUrl = dbProperties.getDataSourceUrl();
        String dataSourceUser = dbProperties.getDataSourceUser();
        String dataSourcePassword = dbProperties.getDataSourcePassword();
        ConnectionProperties connectionProperties = dbProperties.getConnectionProperties();
        assertEquals("com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager", providerClass);
        assertEquals("", dbType);
        assertEquals(30, queryTimeoutInSecs);
        assertEquals("com.mysql.jdbc.jdbc2.optional.MysqlDataSource", dataSourceClassName);
        assertEquals("jdbc:mysql://localhost/schema_registry", dataSourceUrl);
        assertNull(dataSourceUser);
        assertEquals("registry_password", dataSourcePassword);
        assertNull(connectionProperties);
        
    }

    @Test
    public void testReadIncorrectYaml() throws Exception {
        final File yml = new File(Resources.getResource("storageconfiginvalidtest.yaml").toURI());
        assertThrows(ConfigurationParsingException.class, () -> factory.build(yml));
    }
}
