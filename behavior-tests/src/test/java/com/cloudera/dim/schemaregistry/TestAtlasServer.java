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

import com.cloudera.dim.schemaregistry.atlas.MockAtlasApplication;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestAtlasServer extends AbstractTestServer {

    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasServer.class);

    private final ExecutorService threadPool = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("atlas-%d").setDaemon(true).build());
    private int atlasPort;

    private static TestAtlasServer instance;
    private MockAtlasApplication atlasApplication;

    public static TestAtlasServer getInstance() {
        if (instance == null) {
            synchronized (TestAtlasServer.class) {
                if (instance == null) {
                    instance = new TestAtlasServer();
                }
            }
        }
        return instance;
    }

    public int getAtlasPort() {
        return atlasPort;
    }

    @Override
    public void start() throws Exception {
        boolean alreadyStarted = started.getAndSet(true);
        if (alreadyStarted) {
            return;
        }

        this.atlasPort = findFreePort();
        String atlasYamlTxt = configGenerator.generateAtlasYaml(atlasPort);
        File atlasYaml = writeYamlFile("atlas", atlasYamlTxt);
        LOG.debug("atlas.yaml file generated at {}", atlasYaml.getAbsolutePath());

        this.atlasApplication = new MockAtlasApplication();

        Future<Boolean> atlasStarted = threadPool.submit(() -> {
            try {
                atlasApplication.run("server", atlasYaml.getAbsolutePath());
                return true;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        atlasStarted.get(1, TimeUnit.MINUTES);
        this.running.set(true);
    }

    public void stop() throws Exception {
        try {
            atlasApplication.stop();
        } catch (Exception ex) { }
        try {
            threadPool.shutdown();
        } catch (Exception ex) { }
        super.stop();
    }

}
