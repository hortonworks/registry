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
package com.cloudera.dim.atlas.bootstrap;

import com.cloudera.dim.atlas.AtlasPlugin;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hortonworks.registries.common.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.net.ConnectException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AtlasModelBootstrap implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasModelBootstrap.class);

    private static final int MAX_ATTEMPTS_TO_CONNECT_TO_ATLAS = 3;
    private final ExecutorService threadPool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("atlas-bootstrap-%d").build());

    private final AtlasConfiguration atlasConfiguration;
    private final AtlasPlugin atlasPlugin;

    @Inject
    public AtlasModelBootstrap(@Nullable AtlasConfiguration atlasConfiguration, AtlasPlugin atlasPlugin) {
        this.atlasConfiguration = atlasConfiguration;
        this.atlasPlugin = atlasPlugin;

        if (atlasConfiguration != null && atlasConfiguration.isEnabled()) {
            threadPool.submit(this);
        }
    }

    @Override
    public void run() {
        if (atlasIsRunning()) {
            setupModel();
        }
    }

    /** On cluster startup it's possible that Atlas takes a few minutes longer than SR to startup. */
    private synchronized boolean atlasIsRunning() {
        int retryCount = 0;
        boolean retryLater = true;

        while (retryLater && !Thread.interrupted()) {
            retryLater = false;
            retryCount++;
            try {
                LOG.debug("Checking if we can connect to Atlas");
                checkNetworkConnection();
            } catch (ConnectException cex) {
                if (retryCount <= MAX_ATTEMPTS_TO_CONNECT_TO_ATLAS) {
                    retryLater = true;
                } else {
                    LOG.error("Failed to connect to Atlas after {} attempts.", retryCount, cex);
                    return false;
                }
            }

            if (retryLater) {
                try {
                    wait(60000);
                } catch (InterruptedException iex) {
                    LOG.info("Atlas model bootstrapping was interrupted.", iex);
                }
            }
        }

        return true;
    }

    // let's send a random request to Atlas
    private void checkNetworkConnection() throws ConnectException {
        try {
            atlasPlugin.isAtlasModelInitialized();
        } catch (Exception ex) {
            Throwable cause = ex;
            while ((cause = cause.getCause()) != null) {
                if (cause instanceof ConnectException) {
                    throw (ConnectException) cause;
                }
            }
        }
    }

    private void setupModel() {
        boolean atlasModelInitialized = false;
        boolean kafkaSchemaModelInitialized = false;
        try {
            atlasModelInitialized = atlasPlugin.isAtlasModelInitialized();
            kafkaSchemaModelInitialized = atlasPlugin.isKafkaSchemaModelInitialized();
        } catch (Exception t) {
            LOG.debug("Exception while querying if the model exists. This can be ignored.", t);
        }
        if (atlasModelInitialized && kafkaSchemaModelInitialized) {
            LOG.info("Atlas model is set up.");
            return;   // nothing to do
        }

        LOG.info("Atlas model bootstrap has started.");
        if (!atlasModelInitialized) {
            try {
                setupRegistryModel();
            } catch (Exception ex) {
                LOG.error("Error while setting up the Schema Registry model in Atlas.", ex);
            }
        }
        if (!kafkaSchemaModelInitialized) {
            try {
                setupKafkaModel();
            } catch (Exception ex) {
                LOG.error("Error while setting up the connection to Kafka topics in Atlas.", ex);
            }
        }
    }

    private void setupKafkaModel() {
        atlasPlugin.setupKafkaSchemaModel();
    }

    private void setupRegistryModel() {
        atlasPlugin.setupAtlasModel();
    }
}
