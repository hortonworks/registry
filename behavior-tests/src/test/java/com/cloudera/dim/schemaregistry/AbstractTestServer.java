/**
 * Copyright 2016-2021 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.cloudera.dim.schemaregistry;

import com.cloudera.dim.schemaregistry.config.TestConfigGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

abstract class AbstractTestServer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTestServer.class);

    protected final AtomicBoolean running = new AtomicBoolean(false);
    protected final AtomicBoolean started = new AtomicBoolean(false);
    protected final TestConfigGenerator configGenerator = new TestConfigGenerator();

    public boolean isRunning() {
        return running.get();
    }

    public abstract void start() throws Exception;

    public void stop() throws Exception {
        running.set(false);
        started.set(false);
    }

    protected int findFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (Exception ex) {
            LOG.warn("Could not find free port.", ex);
            return 0;
        }
    }

    protected File writeFile(String prefix, String suffix, String content) throws IOException {
        File tmpDir;
        do {
            tmpDir = new File(FileUtils.getTempDirectory(), RandomStringUtils.randomAlphabetic(8));
        } while (tmpDir.exists());
        tmpDir.mkdirs();

        File tmpFile = new File(tmpDir, prefix + suffix);
        Files.write(tmpFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return tmpFile;
    }

}
