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
package com.hortonworks.registries.common.util;

import com.google.common.io.ByteStreams;
import com.hortonworks.registries.common.FileStorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Implementation of JarStorage interface backed by local file system
 */
public class LocalFileSystemStorage implements FileStorage {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemStorage.class);

    // the configuration keys
    public static final String CONFIG_DIRECTORY = "directory";

    private String directory = DEFAULT_DIR;
    
    public LocalFileSystemStorage(FileStorageConfiguration config) {
        String dir;
        if ((dir = config.getProperties().getDirectory()) != null) {
            directory = dir;
        }
        ensureDirExists();
    }
    

    protected void ensureDirExists() {
        final File dirFile = FileSystems.getDefault().getPath(directory).toFile();

        if (!dirFile.exists()) {
            if (!dirFile.mkdirs()) {
                throw new RuntimeException("Directory " + directory + " could not be created");
            }
        } else {
            if (!dirFile.isDirectory()) {
                throw new RuntimeException("Given directory path " + directory + " is not a directory");
            }
        }
    }

    @Override
    public String upload(InputStream inputStream, String name) throws IOException {
        ensureDirExists();

        Path path = FileSystems.getDefault().getPath(directory, name);
        File file = path.toFile();
        if (!file.createNewFile()) {
            throw new IOException("File: [" + name + "] already exists");
        }
        try (OutputStream outputStream = new FileOutputStream(file)) {
            ByteStreams.copy(inputStream, outputStream);
        }
        return path.toString();
    }

    @Override
    public InputStream download(String name) throws IOException {
        ensureDirExists();

        Path path = FileSystems.getDefault().getPath(directory, name);
        File file = path.toFile();
        return new FileInputStream(file);
    }

    @Override
    public boolean delete(String name) throws IOException {
        try {
            ensureDirExists();

            Path path = FileSystems.getDefault().getPath(directory, name);
            return Files.deleteIfExists(path);
        } catch (IOException iex) {
            throw iex;
        } catch (Exception ex) {
            LOG.error("Could not delete " + directory + "/" + name, ex);
            return false;
        }
    }

    @Override
    public boolean exists(String name) {
        return Files.exists(FileSystems.getDefault().getPath(directory, name));
    }
}
