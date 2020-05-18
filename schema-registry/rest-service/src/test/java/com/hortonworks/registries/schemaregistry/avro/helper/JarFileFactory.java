package com.hortonworks.registries.schemaregistry.avro.helper;

import java.io.File;
import java.io.FileInputStream;
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
}
