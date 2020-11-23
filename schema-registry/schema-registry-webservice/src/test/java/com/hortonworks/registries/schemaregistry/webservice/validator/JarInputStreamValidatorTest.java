package com.hortonworks.registries.schemaregistry.webservice.validator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.hortonworks.registries.schemaregistry.avro.helper.JarFileFactory;
import com.hortonworks.registries.schemaregistry.webservice.validator.exception.InvalidJarFileException;

public class JarInputStreamValidatorTest {
    private JarInputStreamValidator underTest = new JarInputStreamValidator();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testValidateWhenValidJarAccepts() throws Exception {
        // given
        InputStream validJar = new FileInputStream(JarFileFactory.createValidJar());

        // when
        underTest.validate(validJar);

        // then no exception is thrown
    }

    @Test
    public void testValidateWhenCorruptedJarExceptionIsThrown() throws Exception {
        // given
        exceptionRule.expect(InvalidJarFileException.class);
        exceptionRule.expectMessage("Jar file corrupted");
        InputStream corruptedJar = new FileInputStream(JarFileFactory.createCorruptedJar());

        // when
        underTest.validate(corruptedJar);
    }

    @Test
    public void testStreamNotExhausted() throws Exception {
        // given
        File validJar = JarFileFactory.createValidJar();
        try (InputStream copyOfInitialStream = new FileInputStream(validJar);
                InputStream validJarStream = new FileInputStream(validJar);) {
            // when
            InputStream actual = underTest.validate(validJarStream);

            // then
            Assert.assertTrue(IOUtils.contentEquals(actual, copyOfInitialStream));
        }
    }

    @Test
    public void testWhenEmptyZipExceptionIsThrown() throws Exception {
        // given
        exceptionRule.expect(InvalidJarFileException.class);
        exceptionRule.expectMessage("Jar file corrupted");
        byte[] emptyZip = { 80, 75, 05, 06, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00 };
        InputStream emptyZipStream = new ByteArrayInputStream(emptyZip);

        // when
        underTest.validate(emptyZipStream);
    }
}
