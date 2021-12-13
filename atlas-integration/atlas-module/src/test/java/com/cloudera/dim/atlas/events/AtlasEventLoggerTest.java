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
 */
package com.cloudera.dim.atlas.events;

import com.cloudera.dim.atlas.AtlasPlugin;
import com.hortonworks.registries.common.AtlasConfiguration;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable.EventType;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
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

import static com.hortonworks.registries.schemaregistry.AtlasEventStorable.EventType.CREATE_META;
import static com.hortonworks.registries.schemaregistry.AtlasEventStorable.PROCESSED_ID;
import static com.hortonworks.registries.schemaregistry.AtlasEventStorable.TYPE;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class AtlasEventLoggerTest {

    private static final String ATLAS_EVENTS = "atlas_events";

    private HikariCPConnectionBuilder h2;
    private JdbcStorageManager storageManager;


    @BeforeEach
    public void setUp() throws Exception {
        h2 = new HikariCPConnectionBuilder(getDbConnectionConfig());
        createTable();

        ExecutionConfig config = new ExecutionConfig(3, DatabaseType.MYSQL);
        QueryExecutor queryExecutor = new MySqlExecutor(config, h2);
        storageManager = new JdbcStorageManager(queryExecutor);
        storageManager.registerStorables(singletonList(AtlasEventStorable.class));
    }

    private AtlasEventLogger atlasEventLogger() {
        AtlasConfiguration atlasConfiguration = new AtlasConfiguration();
        atlasConfiguration.setWaitBetweenAuditProcessing(3000L);
        atlasConfiguration.setEnabled(false);
        AtlasPlugin atlasPlugin = Mockito.mock(AtlasPlugin.class);
        Authorizer.UserAndGroups user = new Authorizer.UserAndGroups("user1", emptySet());
        return new AtlasEventLogger(atlasConfiguration, atlasPlugin, storageManager, storageManager).withAuth(user);
    }

    @AfterEach
    public void tearDown() throws Exception {
        try (Connection connection = h2.getConnection();
             Statement stmt = connection.createStatement()
        ) {
            stmt.execute("DROP TABLE " + ATLAS_EVENTS);
            connection.commit();
        }
        storageManager.cleanup();
        h2.cleanup();
    }

    private static HikariConfig getDbConnectionConfig() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(Driver.class.getName());
        String jdbcUrl = "jdbc:h2:mem:test;MODE=MYSQL;DATABASE_TO_UPPER=FALSE;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");
        hikariConfig.setAutoCommit(true);
        hikariConfig.setConnectionInitSql("");
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setMaximumPoolSize(30);
        hikariConfig.setMinimumIdle(10);
        hikariConfig.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        return hikariConfig;
    }

    private void createTable() throws SQLException {
        try (
                Connection connection = h2.getConnection();
                Statement stmt = connection.createStatement()
        ) {
            stmt.execute("CREATE TABLE " + ATLAS_EVENTS + " (" +
                    "  id              BIGINT AUTO_INCREMENT NOT NULL, " +
                    "  username        VARCHAR(255)          NOT NULL, " +
                    "  processedId     BIGINT                NOT NULL," +
                    "  type            INT                   NOT NULL," +
                    "  processed       BOOLEAN               NOT NULL," +
                    "  failed          BOOLEAN               NOT NULL," +
                    "  timestamp       BIGINT                NOT NULL," +
                    "  UNIQUE KEY (id)" +
                    ")");
            connection.commit();
        }
    }

    private void assertAtlasEntry(Long id, EventType type) {
        try (
                Connection connection = h2.getConnection();
                PreparedStatement pstmt = connection.prepareStatement("select " + PROCESSED_ID + "," + TYPE + " from " + ATLAS_EVENTS);
                ResultSet rs = pstmt.executeQuery()
        ) {
            if (!rs.next()) {
                fail("No data was found in the database.");
            }

            Long actualId = rs.getLong(1);
            int actualTypeNum = rs.getInt(2);
            EventType actualType = EventType.forNumValue(actualTypeNum);

            assertEquals(id, actualId);
            assertEquals(type, actualType);
            connection.commit();
        } catch (Exception exception) {
            fail(exception);
        }
    }

    @Test
    public void testAddSchemaMetadata() {
        try (AtlasEventLogger atlasEventLogger = atlasEventLogger()) {
            // given
            Long id = RandomUtils.nextLong();


            // when
            atlasEventLogger.createMeta(id);

            // then
            assertAtlasEntry(id, CREATE_META);
        }
    }

    @Test
    public void testUpdateSchemaMetadata() {
        try (AtlasEventLogger atlasEventLogger = atlasEventLogger()) {
            // given
            Long id = RandomUtils.nextLong();

            // when
            atlasEventLogger.updateMeta(id);

            // then
            assertAtlasEntry(id, EventType.UPDATE_META);
        }
    }

    @Test
    public void testAddSchemaVersion() {
        try (AtlasEventLogger atlasEventLogger = atlasEventLogger()) {
            // given
            Long id = RandomUtils.nextLong();

            // when
            atlasEventLogger.createVersion(id);

            // then
            assertAtlasEntry(id, EventType.CREATE_VERSION);
        }
    }
}
