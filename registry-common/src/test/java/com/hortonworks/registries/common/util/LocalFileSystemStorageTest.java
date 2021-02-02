/**
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.common.util;

import com.hortonworks.registries.common.FileStorageConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.util.Resources;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.validation.Validator;
import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalFileSystemStorageTest extends AbstractFileStorageTest {

    private final String uploadDir;

    private final ObjectMapper objectMapper = Jackson.newObjectMapper();
    private final Validator validator = Validators.newValidator();
    private final YamlConfigurationFactory<FileStorageConfiguration> factory =
            new YamlConfigurationFactory(FileStorageConfiguration.class, validator, objectMapper, "");

    public LocalFileSystemStorageTest() {
        try {
            uploadDir = Files.createTempDirectory("upload").toFile().getAbsolutePath();
        } catch (Exception ex) {
            throw new RuntimeException("Could not create temporary directory for uploading files. ", ex);
        }
    }

    @AfterEach
    public void tearDown() {
        FileUtils.deleteQuietly(new File(uploadDir));
        FileUtils.deleteQuietly(new File(FileStorage.DEFAULT_DIR));
    }

    @Override
    public FileStorage getFileStorage() {
        FileStorageConfiguration config = new FileStorageConfiguration();
        config.getProperties().setDirectory(uploadDir);
        return new LocalFileSystemStorage(config);
    }
    
    @Test
    public void testReadYaml() throws Exception {
        final File yml = new File(Resources.getResource("testconfig.yaml").toURI());
        final FileStorageConfiguration wid = factory.build(yml);
        String directory = wid.getProperties().getDirectory();
        String className = wid.getClassName();
        assertEquals("/tmp/schema-registry/test/directory", directory);
        assertEquals("com.hortonworks.registries.common.util.LocalFileSystemStorage", className);
    }
}
