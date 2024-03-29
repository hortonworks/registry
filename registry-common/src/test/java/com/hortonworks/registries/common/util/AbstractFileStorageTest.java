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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

public abstract class AbstractFileStorageTest {


    public abstract FileStorage getFileStorage();

    @Test
    public void testJarStorage() throws IOException {
        FileStorage fileStorage = getFileStorage();
        File file = File.createTempFile("test", ".tmp");
        file.deleteOnExit();
        List<String> lines = Lists.newArrayList("test-line-1", "test-line-2");
        Files.write(file.toPath(), lines, Charset.forName("UTF-8"));
        String name = "file.name";

        // delete the file if it already exists
        fileStorage.delete(name);

        fileStorage.upload(new FileInputStream(file), name);
        InputStream inputStream = fileStorage.download(name);

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String nextLine;
        List<String> actual = Lists.newArrayList();
        while ((nextLine = bufferedReader.readLine()) != null) {
            actual.add(nextLine);
        }
        Assertions.assertEquals(lines, actual);
    }
}
