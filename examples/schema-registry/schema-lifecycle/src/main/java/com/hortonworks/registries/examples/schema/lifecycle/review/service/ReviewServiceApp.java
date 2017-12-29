/*
 * Copyright 2017 Hortonworks.
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
 */

package com.hortonworks.registries.examples.schema.lifecycle.review.service;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

import java.util.HashMap;
import java.util.Map;

public class ReviewServiceApp extends Application<ReviewServiceConfig> {

    @Override
    public void run(ReviewServiceConfig config, Environment environment) throws Exception {
        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(createConfig(config.getSchemaRegistryUrl()));
        environment.jersey().register(new ReviewServiceResource(schemaRegistryClient));
    }

    public static void main(String[] args) throws Exception {
        ReviewServiceApp reviewServiceApp = new ReviewServiceApp();
        reviewServiceApp.run("server", ReviewServiceApp.class.getClassLoader().getResource("review-service.yaml").getPath());
    }

    public static Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000L);
        return config;
    }
}
