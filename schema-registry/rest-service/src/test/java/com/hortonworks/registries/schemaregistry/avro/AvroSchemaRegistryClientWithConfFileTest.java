/*
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.commons.io.IOUtils;
import org.junit.Before;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 *
 */
public class AvroSchemaRegistryClientWithConfFileTest extends AvroSchemaRegistryClientTest {

    @Before
    public void setup() throws IOException {

        Path confFilePath = Files.createTempFile("sr-client-conf", ".props");
        File confFile = confFilePath.toFile();
        try (FileOutputStream fos = new FileOutputStream(confFile);
             InputStream fis = this.getClass().getResourceAsStream("/schema-registry-client.yaml")) {
            String confText = IOUtils.toString(fis, "UTF-8").replace("__registry_url", rootUrl);
            IOUtils.write(confText,
                          fos,
                          "UTF-8");
        }
        schemaRegistryClient = new SchemaRegistryClient(confFile);
    }

}
