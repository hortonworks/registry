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

import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class LocalFileSystemStorageTest extends AbstractFileStorageTest {

    private final String UPLOAD_DIR;

    public LocalFileSystemStorageTest() {
        try {
            UPLOAD_DIR = Files.createTempDirectory("upload").toFile().getAbsolutePath();
        } catch (Exception ex) {
            throw new RuntimeException("Could not create temporary directory for uploading files. ", ex);
        }
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(new File(UPLOAD_DIR));
        FileUtils.deleteQuietly(new File(FileStorage.DEFAULT_DIR));
    }

    @Override
    public FileStorage getFileStorage() {
        Map<String, String> config = new HashMap<>();
        config.put(LocalFileSystemStorage.CONFIG_DIRECTORY, UPLOAD_DIR);

        LocalFileSystemStorage localFileSystemStorage = new LocalFileSystemStorage();
        localFileSystemStorage.init(config);
        return localFileSystemStorage;
    }
}
