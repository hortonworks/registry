/**
 * Copyright 2016-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
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
