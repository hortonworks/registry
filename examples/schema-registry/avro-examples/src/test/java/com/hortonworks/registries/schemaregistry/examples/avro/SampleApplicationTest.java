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
package com.hortonworks.registries.schemaregistry.examples.avro;

import com.hortonworks.registries.webservice.RegistryApplication;
import com.hortonworks.registries.webservice.RegistryConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

@ExtendWith(DropwizardExtensionsSupport.class)
public class SampleApplicationTest {

    private static final DropwizardAppExtension<RegistryConfiguration> DROPWIZARD_APP_RULE =
            new DropwizardAppExtension<>(RegistryApplication.class, ResourceHelpers.resourceFilePath("schema-registry-test.yaml"));

    @Test
    public void testApis() throws Exception {
        final String rootUrl = String.format("http://localhost:%d/api/v1", DROPWIZARD_APP_RULE.getLocalPort());
        Map<String, Object> config = SampleSchemaRegistryClientApp.createConfig(rootUrl);
        SampleSchemaRegistryClientApp sampleSchemaRegistryClientApp = new SampleSchemaRegistryClientApp(config);

        sampleSchemaRegistryClientApp.runSchemaApis();
        sampleSchemaRegistryClientApp.runCustomSerDesApi();
        sampleSchemaRegistryClientApp.runAvroSerDesApis();
    }

}
