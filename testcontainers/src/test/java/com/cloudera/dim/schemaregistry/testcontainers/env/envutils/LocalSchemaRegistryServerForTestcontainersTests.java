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

package com.cloudera.dim.schemaregistry.testcontainers.env.envutils;

import com.cloudera.dim.schemaregistry.AbstractTestServer;
import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbConnProps;
import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbHandler;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LocalSchemaRegistryServerForTestcontainersTests extends AbstractTestServer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSchemaRegistryServerForTestcontainersTests.class);
    private final String yamlFilePath;
    private ExecutorService threadPool;
    private final DbHandler dbHandler;
    private DataSource dataSource;

    @Getter
    private int schemaRegistryPort;


    private LocalSchemaRegistryServer localSchemaRegistry;

    private final DbConnProps dbConnProps;

    @Getter
    private final TestSetup testSetup;


    public LocalSchemaRegistryServerForTestcontainersTests(DbConnProps dbConnProps,
                                                           TestSetup testSetup,
                                                           String yamlFilePath,
                                                           int schemaRegistryPort,
                                                           DbHandler dbHandler
    ) {
        this.dbConnProps = dbConnProps;
        this.testSetup = testSetup;
        this.yamlFilePath = yamlFilePath;
        this.schemaRegistryPort = schemaRegistryPort;
        this.dbHandler = dbHandler;
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

        dbHandler.populateDatabase(dbConnProps,
                testSetup.getTempFolderPath());

        dataSource = dbHandler.getDataSource();

        this.localSchemaRegistry = new LocalSchemaRegistryServer(yamlFilePath);
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
        } catch (Exception ex) {
        }
        try {
            dbHandler.stop();
        } catch (Exception ex) {
        }
        try {
            threadPool.shutdown();
            threadPool = null;
        } catch (Exception ex) {
        }
        super.stop();
    }

    public void cleanupDb() {
        dbHandler.resetDatabase(dbConnProps,
                testSetup.getTempFolderPath());
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

}
