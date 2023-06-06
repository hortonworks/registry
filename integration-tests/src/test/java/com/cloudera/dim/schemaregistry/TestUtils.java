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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils() {
    }

    public static File preprocessMigrationsForH2(File bootstrapDir) throws IOException {
        File tmpDir = Files.createTempDirectory("srtest").toFile();
        return preprocessMigrationsForH2(bootstrapDir, tmpDir.getAbsolutePath());
    }

    /**
     * The DDL files under bootstrap need to be sanitized before we can pass them to H2.
     */
    public static File preprocessMigrationsForH2(File bootstrapDir, String outputDirPath) throws IOException {
        Files.createDirectories(Paths.get(outputDirPath));

        for (File file : bootstrapDir.listFiles()) {
            if (file.getName().startsWith("v006")) {
                continue;
            }
            List<String> lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
            File outFile = new File(outputDirPath, file.getName());

            try (PrintWriter out = new PrintWriter(new FileWriter(outFile))) {
                boolean ignore = false;
                for (String line : lines) {
                    // ignore commented lines or lines which contain procedure operations
                    if (line.startsWith("--") || line.contains("Cloudera") || line.toUpperCase().contains("CALL")
                            || line.toUpperCase().contains("DROP PROCEDURE")
                            // H2 does not support table locks
                            || line.toUpperCase().contains("LOCK TABLE")) {
                        continue;
                    }
                    // H2 has no support for MySql functions, but luckily we can ignore them
                    if (!ignore && line.toLowerCase().contains("delimiter")) {
                        ignore = true;
                    } else if (ignore && line.toLowerCase().contains("delimiter")) {
                        ignore = false;
                    } else if (!ignore) {
                        out.println(line);
                    }
                }
            } catch (Exception ex) {
                LOG.error("Failure while writing file {}", file.getName(), ex);
            }
            outFile.deleteOnExit();

            LOG.debug("Wrote preprocessed DDL file {}", outFile.getAbsolutePath());
        }

        return new File(outputDirPath);
    }


    public static int findFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (Exception ex) {
            throw new RuntimeException("Could not find free port.", ex);
        }
    }

    /**
     * Recursively search all subdirectories.
     */
    public static File findSubdir(File root, String name) {
        if (root.getName().equals(name)) {
            return root;
        } else {
            File[] files = root.listFiles();
            if (files == null) {
                return null;
            }
            for (File f : files) {
                if (f.isDirectory()) {
                    File subdir = findSubdir(f, name);
                    if (subdir != null) {
                        return subdir;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Usually bootstrap dir should be under the root, but let's try to play safer and look at a few other places too.
     */
    public static File getPathToBootstrap(String dbType) throws FileNotFoundException {
        // we want to find the path to /bootstrap/sql/mysql

        File[] files = {
                new File("bootstrap/sql/" + dbType),
                new File(System.getProperty("user.dir"), "bootstrap/sql/" + dbType),
                new File(System.getProperty("user.home"), "bootstrap/sql/" + dbType),
                new File("../bootstrap/sql/" + dbType),
                new File("../../bootstrap/sql/" + dbType),
                new File("../../../bootstrap/sql/" + dbType)
        };

        for (File file : files) {
            try {
                if (file.exists() && file.isDirectory()) {
                    LOG.debug("Bootstrap directory: {}", file.getAbsolutePath());
                    return file;
                }
            } catch (Throwable ex) {
                LOG.trace("Unexpected error for " + file, ex);
            }
        }

        throw new FileNotFoundException("Could not find bootstrap directory near " + new File(".").getAbsolutePath());
    }


    public static String getResourceAsTempFile(Class resourceClass, String resourcePath) throws IOException {
        File tmpFile = File.createTempFile("tmpFile", ".jks");
        String tmpFilePath = tmpFile.getAbsolutePath();
        tmpFile.delete();
        return getResourceAsTempFile(resourceClass, resourcePath, tmpFilePath);
    }

    public static String getResourceAsTempFile(Class resourceClass, String resourcePath, String targetFilePath) {
        Path path = Paths.get(targetFilePath);
        try {
            Files.createDirectories(path.getParent());
            String absolutePath = Files.createFile(path)
                    .toAbsolutePath().toString();
            byte[] resourceBytes = IOUtils.toByteArray(resourceClass.getResource(resourcePath));
            try (FileOutputStream out = new FileOutputStream(absolutePath)) {
                IOUtils.write(resourceBytes, out);
            }
            if (File.separator.equals("\\")) {
                // fix windows file path
                absolutePath = absolutePath.replaceAll(Pattern.quote(File.separator), "/");
            }
            return absolutePath;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File writeFileToTempDir(String prefix, String suffix, String content) {
        File tmpFile;
        try {
            tmpFile = File.createTempFile(prefix, suffix);
            Files.write(tmpFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tmpFile;
    }

    public static File writeFileToTempDir(String prefix, String suffix, String content, String outputDirPath) {
        try {
            Path directories = Files.createDirectories(Paths.get(outputDirPath));
            String absolutePath = Files.createFile(Paths.get(outputDirPath, prefix + suffix))
                    .toAbsolutePath().toString();
            File tmpFile = new File(absolutePath);
            Files.write(tmpFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
            return tmpFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
