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

import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.common.FileStorageConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;
import java.nio.file.Files;

public class LocalFileSystemStorageTest extends AbstractFileStorageTest {

    private final String uploadDir;

    public LocalFileSystemStorageTest() {
        try {
            uploadDir = Files.createTempDirectory("upload").toFile().getAbsolutePath();
        } catch (Exception ex) {
            throw new RuntimeException("Could not create temporary directory for uploading files. ", ex);
        }
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(new File(uploadDir));
        FileUtils.deleteQuietly(new File(FileStorage.DEFAULT_DIR));
    }

    @Override
    public FileStorage getFileStorage() {
        FileStorageConfiguration config = new FileStorageConfiguration();
        config.setProperties(ImmutableMap.of(LocalFileSystemStorage.CONFIG_DIRECTORY, uploadDir));

        return new LocalFileSystemStorage(config);
    }
}
