/**
 * Copyright 2017-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.avro;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.hortonworks.registries.shaded.javax.ws.rs.BadRequestException;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.helper.JarFileFactory;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.util.SchemaRegistryTestName;

@Category(IntegrationTest.class)
public class FileUploadTest {
    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;
    private static SchemaRegistryClient SCHEMA_REGISTRY_CLIENT;
    @Rule
    public SchemaRegistryTestName testNameRule = new SchemaRegistryTestName();

    @BeforeClass
    public static void setup() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(
                SchemaRegistryTestProfileType.DEFAULT);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        SCHEMA_REGISTRY_CLIENT = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient();
    }

    private String uploadValidFile() throws IOException {
        InputStream inputStream = new FileInputStream(JarFileFactory.createValidJar());
        return SCHEMA_REGISTRY_CLIENT.uploadFile(inputStream);
    }

    private String uploadInvalidFile() throws IOException {
        InputStream inputStream = new FileInputStream(JarFileFactory.createCorruptedJar());
        return SCHEMA_REGISTRY_CLIENT.uploadFile(inputStream);
    }

    @Test
    public void uploadValidFileResults() throws Exception {
        // when
        uploadValidFile();
        
        // then no exception is thrown
    }

    @Test(expected = BadRequestException.class)
    public void uploadInvalidFileResultsBadRequest() throws Exception {
        // when
        uploadInvalidFile();
    }
}
