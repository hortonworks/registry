/*
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
package com.cloudera.dim.atlas.events;

import com.cloudera.dim.atlas.AtlasPlugin;
import com.hortonworks.registries.common.AtlasConfiguration;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable.EventType;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.factory.MySqlExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.zaxxer.hikari.HikariConfig;
import org.apache.commons.lang3.RandomUtils;
import org.h2.Driver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import static com.hortonworks.registries.schemaregistry.AtlasEventStorable.PROCESSED_ID;
import static com.hortonworks.registries.schemaregistry.AtlasEventStorable.TYPE;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class AtlasEventLoggerTest {

    private static final String ATLAS_EVENTS = "atlas_events";
    private static final String NAMESPACE_SEQUENCE = "namespace_sequence";

    private static final HikariConfig DB_CONFIG = getDbConnectionConfig();

    private HikariCPConnectionBuilder h2;
    private JdbcStorageManager storageManager;
    private AtlasEventLogger atlasEventLogger;

    @BeforeEach
    public void setUp() throws Exception {
        this.h2 = new HikariCPConnectionBuilder(DB_CONFIG);
        createTable();

        AtlasConfiguration atlasConfiguration = new AtlasConfiguration();
        AtlasPlugin atlasPlugin = Mockito.mock(AtlasPlugin.class);
        ExecutionConfig config = new ExecutionConfig(3, DatabaseType.MYSQL);
        QueryExecutor queryExecutor = new MySqlExecutor(config, h2);
        storageManager = new JdbcStorageManager(queryExecutor);
        storageManager.registerStorables(singletonList(AtlasEventStorable.class));

        TransactionManager txManager = Mockito.mock(TransactionManager.class);

        this.atlasEventLogger = new AtlasEventLogger(atlasConfiguration, atlasPlugin, storageManager, txManager);
    }

    @AfterEach
    public void tearDown() throws Exception {
        try (Connection connection = h2.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM " + ATLAS_EVENTS);
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
            stmt.execute("CREATE TABLE IF NOT EXISTS " + ATLAS_EVENTS + " (" +
                    "  id              BIGINT AUTO_INCREMENT NOT NULL, " +
                    "  username        VARCHAR(255)          NOT NULL, " +
                    "  processedId     BIGINT                NOT NULL," +
                    "  type            INT                   NOT NULL," +
                    "  processed       BOOLEAN               NOT NULL," +
                    "  failed          BOOLEAN               NOT NULL," +
                    "  timestamp       BIGINT                NOT NULL," +
                    "  UNIQUE KEY (id)" +
                    ")");
            stmt.close();
        }
    }
    private void assertAtlasEntry(Long id, EventType type) {
        try (Connection connection = this.h2.getConnection()) {
            PreparedStatement pstmt = connection.prepareStatement("select " + PROCESSED_ID + "," + TYPE + " from " + ATLAS_EVENTS);
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) {
                fail("No data was found in the database.");
            }

            Long actualId = rs.getLong(1);
            int actualTypeNum = rs.getInt(2);
            EventType actualType = EventType.forNumValue(actualTypeNum);

            assertEquals(id, actualId);
            assertEquals(type, actualType);
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
    public void testAddSchemaMetadata() {
        // given
        Long id = RandomUtils.nextLong();
        Authorizer.UserAndGroups user = new Authorizer.UserAndGroups("user1", new HashSet<>());

        // when
        atlasEventLogger.withAuth(user).createMeta(id);

        // then
        assertAtlasEntry(id, EventType.CREATE_META);

    }
    
    @Test
    public void testUpdateSchemaMetadata() {
        // given
        Long id = RandomUtils.nextLong();
        Authorizer.UserAndGroups user = new Authorizer.UserAndGroups("martin", new HashSet<>());

        // when
        atlasEventLogger.withAuth(user).updateMeta(id);

        // then
        assertAtlasEntry(id, EventType.UPDATE_META);
    }

    @Test
    public void testAddSchemaVersion() {
        // given
        Long id = RandomUtils.nextLong();
        Authorizer.UserAndGroups user = new Authorizer.UserAndGroups("abigail", new HashSet<>());

        // when
        atlasEventLogger.withAuth(user).createVersion(id);

        // then
        assertAtlasEntry(id, EventType.CREATE_VERSION);
    }

}
