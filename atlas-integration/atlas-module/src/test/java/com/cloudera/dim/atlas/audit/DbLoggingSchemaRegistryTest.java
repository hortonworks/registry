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
package com.cloudera.dim.atlas.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeStrategy;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.factory.MySqlExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.zaxxer.hikari.HikariConfig;
import org.h2.Driver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class DbLoggingSchemaRegistryTest {

    private static final String REGISTRY_AUDIT = "registry_audit";

    private static final HikariConfig DB_CONFIG = getDbConnectionConfig();
    private HikariCPConnectionBuilder h2;
    private JdbcStorageManager storageManager;
    private DbLoggingSchemaRegistry dbLoggingSchemaRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper();
    String testName;

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        this.h2 = new HikariCPConnectionBuilder(DB_CONFIG);
        createTable();

        ExecutionConfig config = new ExecutionConfig(3, DatabaseType.MYSQL);
        QueryExecutor queryExecutor = new MySqlExecutor(config, h2);
        this.storageManager = new JdbcStorageManager(queryExecutor);

        this.dbLoggingSchemaRegistry = new DbLoggingSchemaRegistry(storageManager);

        testName = testInfo.getTestMethod().get().getName();
    }

    @AfterEach
    public void tearDown() throws Exception {
        try (Connection connection = h2.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM " + REGISTRY_AUDIT);
            stmt.close();
        }
        h2.cleanup();
        storageManager.cleanup();
    }

    private static HikariConfig getDbConnectionConfig() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(Driver.class.getName());
        hikariConfig.setJdbcUrl("jdbc:h2:mem:test;MODE=MYSQL;DATABASE_TO_UPPER=FALSE;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE");
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");
        hikariConfig.setAutoCommit(true);
        hikariConfig.setConnectionInitSql("");
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setMaximumPoolSize(30);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        return hikariConfig;
    }

    private void createTable() throws SQLException {
        try (Connection connection = h2.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS " + REGISTRY_AUDIT + " (" +
                    "  id              BIGINT AUTO_INCREMENT NOT NULL, " +
                    "  username        VARCHAR(255)          NOT NULL, " +
                    "  processedData  TEXT," +
                    "  processed       BOOLEAN               NOT NULL," +
                    "  failed          BOOLEAN               NOT NULL," +
                    "  timestamp       BIGINT                NOT NULL," +
                    "  UNIQUE KEY (id)" +
                    ")");
            stmt.close();
        }
    }
    private void checkAuditEntry(String expectedMethodName, List<String> expectedTypes) {
        try (Connection connection = this.h2.getConnection()) {
            PreparedStatement pstmt = connection.prepareStatement("select processedData from " + REGISTRY_AUDIT);
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) {
                fail("No data was found in the database.");
            }

            String processedDataJson = rs.getString(1);
            assertNotNull(processedDataJson);

            AuditEntry auditEntry = objectMapper.readValue(processedDataJson, AuditEntry.class);
            assertNotNull(auditEntry);
            assertEquals(expectedMethodName, auditEntry.getMethodName());
            assertEquals(expectedTypes, auditEntry.getParamTypes());
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }
    
    private SchemaMetadata metadataCreator(String name) {
        return new SchemaMetadata.Builder(name)
                .type("avro")
                .schemaGroup("Kafka")
                .description("description")
                .compatibility(SchemaCompatibility.BACKWARD)
                .validationLevel(SchemaValidationLevel.ALL)
                .evolve(false)
                .build();
    }

    @Test
    public void testAddSchemaMetadata() throws Exception {
        // given
        String schemaName = testName;
        SchemaMetadata schemaMetadata = metadataCreator(schemaName);
        List<String> paramTypes = Arrays.asList(SchemaMetadata.class.getName(), Boolean.class.getName());

        // when
        dbLoggingSchemaRegistry.addSchemaMetadata(schemaMetadata, false);

        // then
        checkAuditEntry("addSchemaMetadata", paramTypes);

    }

    @Test
    public void testMergeSchemaVersion() throws Exception {
        //given
        Long schemaId = 1L;
        SchemaVersionMergeStrategy mergeStrategy = SchemaVersionMergeStrategy.OPTIMISTIC;
        boolean disableCanonicalCheck = false;
        List<String> paramTypes = Arrays.asList(Long.class.getName(), SchemaVersionMergeStrategy.class.getName(), Boolean.class.getName());

        //when
        dbLoggingSchemaRegistry.mergeSchemaVersion(schemaId, mergeStrategy, disableCanonicalCheck);

        //then
       checkAuditEntry("mergeSchemaVersion", paramTypes);
    }
    
    @Test
    public void testUpdateSchemaMetadata() throws Exception {
        //given
        String schemaName = testName;
        SchemaMetadata metadata = metadataCreator(schemaName);
        List<String> paramTypes = Arrays.asList(String.class.getName(), SchemaMetadata.class.getName());
        
        //when
        dbLoggingSchemaRegistry.updateSchemaMetadata(schemaName, metadata);
        
        //then
        checkAuditEntry("updateSchemaMetadata", paramTypes);
    }

    @Test
    public void testAddSchemaVersion() throws Exception {
        //given
        String schemaBranchName = "MASTER";
        String schemaName = testName; 
        SchemaVersion schemaVersion = new SchemaVersion("text", "description"); 
        boolean disableCanonicalCheck = false;
        List<String> paramTypes = Arrays.asList(String.class.getName(), String.class.getName(), SchemaVersion.class.getName(), Boolean.class.getName());


        //when
        dbLoggingSchemaRegistry.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, disableCanonicalCheck);

        //then
        checkAuditEntry("addSchemaVersion", paramTypes);
    }

    @Test
    public void testCheckCompatibility() throws Exception {
        //given
        String schemaBranchName = "MASTER"; 
        String schemaName = testName;
        String toSchemaText = "text";
        List<String> paramTypes = Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName());


        //when
        dbLoggingSchemaRegistry.checkCompatibility(schemaBranchName, schemaName, toSchemaText);

        //then
        checkAuditEntry("checkCompatibility", paramTypes);
    }

    @Test
    public void testAddSerDes() throws Exception {
        //given
        SerDesPair serDesPair = new SerDesPair();
        List<String> paramTypes = Arrays.asList(SerDesPair.class.getName());

        //when
        dbLoggingSchemaRegistry.addSerDes(serDesPair);

        //then
        checkAuditEntry("addSerDes", paramTypes);
    }

    @Test
    public void testMapSchemaWithSerDes() throws Exception {
        //given
        String schemaName = testName;
        Long serDesId = 2L;
        List<String> paramTypes = Arrays.asList(String.class.getName(), Long.class.getName());


        //when
        dbLoggingSchemaRegistry.mapSchemaWithSerDes(schemaName, serDesId);

        //then
        checkAuditEntry("mapSchemaWithSerDes", paramTypes);
    }

    @Test
    public void testEnableSchemaVersion() throws Exception {
        //given
        Long schemaVersionId = 3L;
        List<String> paramTypes = Arrays.asList(Long.class.getName());


        //when
        dbLoggingSchemaRegistry.enableSchemaVersion(schemaVersionId);

        //then
        checkAuditEntry("enableSchemaVersion", paramTypes);
    }

    @Test
    public void testDeleteSchemaVersion() throws Exception {
        //given
        Long schemaVersionId = 4L;
        List<String> paramTypes = Arrays.asList(Long.class.getName());

        //when
        dbLoggingSchemaRegistry.deleteSchemaVersion(schemaVersionId);

        //then
        checkAuditEntry("deleteSchemaVersion", paramTypes);
    }

    @Test
    public void testArchiveSchemaVersion() throws Exception {
        //given
        Long schemaVersionId = 5L;
        List<String> paramTypes = Arrays.asList(Long.class.getName());

        //when
        dbLoggingSchemaRegistry.archiveSchemaVersion(schemaVersionId);

        //then
        checkAuditEntry("archiveSchemaVersion", paramTypes);
    }

    @Test
    public void testDisableSchemaVersion() throws Exception {
        //given
        Long schemaVersionId = 6L;
        List<String> paramTypes = Arrays.asList(Long.class.getName());

        //when
        dbLoggingSchemaRegistry.disableSchemaVersion(schemaVersionId);

        //then
        checkAuditEntry("disableSchemaVersion", paramTypes);
    }

    @Test
    public void testStartSchemaVersionReview() throws Exception {
        //given
        Long schemaVersionId = 7L;
        List<String> paramTypes = Arrays.asList(Long.class.getName());

        //when
        dbLoggingSchemaRegistry.startSchemaVersionReview(schemaVersionId);

        //then
        checkAuditEntry("startSchemaVersionReview", paramTypes);
    }

    @Test
    public void testTransitionState() throws Exception {
        //given
        Long schemaVersionId = 8L;
        Byte targetStateId = new Byte("8"); 
        byte[] transitionDetails = new byte[]{};
        List<String> paramTypes = Arrays.asList(Long.class.getName(), Byte.class.getName(), byte[].class.getName());

        //when
        dbLoggingSchemaRegistry.transitionState(schemaVersionId, targetStateId, transitionDetails);

        //then
        checkAuditEntry("transitionState", paramTypes);
    }

    @Test
    public void testCreateSchemaBranch() throws Exception {
        //given
        String branchName = "MASTER";
        String metadataName = testName;
        Long schemaVersionId = 9L;
        SchemaBranch schemaBranch = new SchemaBranch(branchName, metadataName);
        List<String> paramTypes = Arrays.asList(Long.class.getName(), SchemaBranch.class.getName());

        //when
        dbLoggingSchemaRegistry.createSchemaBranch(schemaVersionId, schemaBranch);

        //then
        checkAuditEntry("createSchemaBranch", paramTypes);
    }

}
