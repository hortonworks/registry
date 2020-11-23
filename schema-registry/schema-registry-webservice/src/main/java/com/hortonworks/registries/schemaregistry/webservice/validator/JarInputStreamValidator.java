package com.hortonworks.registries.schemaregistry.webservice.validator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarInputStream;
import java.util.zip.ZipException;

import org.apache.commons.io.IOUtils;

import com.hortonworks.registries.schemaregistry.webservice.validator.exception.InvalidJarFileException;

public class JarInputStreamValidator {

    public InputStream validate(InputStream inputStream) throws IOException, InvalidJarFileException {
        byte[] allBytesFromStream = null;
        try {
            allBytesFromStream = IOUtils.toByteArray(inputStream);
            JarInputStream jarInputStream = new JarInputStream(new ByteArrayInputStream(allBytesFromStream), false);
            if (jarInputStream.getNextJarEntry() == null)
                throw new InvalidJarFileException("Jar file corrupted.");
        } catch (ZipException e) {
            throw new InvalidJarFileException("Jar file corrupted.", e);
        } finally {
            inputStream.close();
        }
        return new ByteArrayInputStream(allBytesFromStream);
    }
}
