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
 */
package com.hortonworks.registries.schemaregistry.avro.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public class JarFileFactory {
    public static File createValidJar() throws IOException, FileNotFoundException {
        File tmpJarFile = Files.createTempFile("foo", ".jar").toFile();
        tmpJarFile.deleteOnExit();
        try (InputStream inputStream = JarFileFactory.class.getClassLoader()
                .getResourceAsStream("serdes-examples.jar.test")) {
            FileUtils.copyInputStreamToFile(inputStream, tmpJarFile);
        }
        return tmpJarFile;
    }

    public static File createCorruptedJar() throws IOException, FileNotFoundException {
        File tmpJarFile = Files.createTempFile("foo", ".jar").toFile();
        tmpJarFile.deleteOnExit();
        try (FileOutputStream fileOutputStream = new FileOutputStream(tmpJarFile)) {
            IOUtils.write(("Some random stuff: " + UUID.randomUUID()).getBytes(), fileOutputStream);
        }
        return tmpJarFile;
    }

    private JarFileFactory() { }
}
