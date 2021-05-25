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
package com.cloudera.dim.schemaregistry.atlas;

import io.dropwizard.Application;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** We run a mocked Atlas REST endpoint. */
public class MockAtlasApplication extends Application<TestAtlasConfiguration> {

    private static final Logger LOG = LoggerFactory.getLogger(MockAtlasApplication.class);

    private volatile Server localServer;

    @Override
    public void run(TestAtlasConfiguration configuration, Environment environment) throws Exception {
        environment.jersey().register(MultiPartFeature.class);
        environment.jersey().register(new AtlasRestResource());

        environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
            @Override
            public void serverStarted(Server server) {
                localServer = server;
                LOG.info("Received callback as server is started :[{}]", server);
            }
        });
    }

    public void stop() {
        if (localServer != null) {
            try {
                localServer.stop();
            } catch (Exception ex) {
                LOG.error("Error while stopping the Atlas server.", ex);
            }
            localServer = null;
        }
    }


}
