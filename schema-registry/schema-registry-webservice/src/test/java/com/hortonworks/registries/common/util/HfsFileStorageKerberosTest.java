/**
 * Copyright 2016-2023 Cloudera, Inc.
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HfsFileStorageKerberosTest {

    private static final Logger LOG = LoggerFactory.getLogger(HfsFileStorageKerberosTest.class);

    private MockFileSystem fileSystem;
    private static MockedStatic<UserGroupInformation> userGroupInformationClass;
    private UserGroupInformation userGroupInformation;
    private static MockedStatic<FileSystem> fileSystemMock;
    private HdfsFileStorage testSubject;

    private static final String FS_URL = "mock://mybucket/mydatalake/myenv";
    private static final String DIRECTORY = "/registry";
    private static final String ADJUSTED_DIRECTORY = HdfsFileStorage.adjustDirectory(FS_URL, DIRECTORY);
    private static final String LOGIN_PRINCIPAL = "schemaregistry/hostname@DOMAIN";
    private static final String LOGIN_KEYTAB =
            "/var/run/cloudera-scm-agent/process/1546340168-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab";
    private static final String JAR_FILE = "serdes.jar";

    @BeforeClass
    public static void setupStaticMocks() {
        userGroupInformationClass = mockStatic(UserGroupInformation.class);
        fileSystemMock = mockStatic(FileSystem.class);
    }

    @Before
    public void setup() throws Exception {
        LOG.debug("Initializing the Hdfs test");
        userGroupInformationClass.reset();
        fileSystemMock.reset();
        testSubject = new HdfsFileStorage();


        userGroupInformation = mock(UserGroupInformation.class);
        fileSystem = new MockFileSystem();

        userGroupInformationClass.when(UserGroupInformation::getLoginUser).thenReturn(userGroupInformation);

        when(userGroupInformation.doAs(any(PrivilegedAction.class))).thenAnswer(invocation ->
                invocation.getArgument(0, PrivilegedAction.class).run());
        when(userGroupInformation.doAs(any(PrivilegedExceptionAction.class))).thenAnswer(invocation ->
                invocation.getArgument(0, PrivilegedExceptionAction.class).run());

        fileSystemMock.when(() -> FileSystem.get(any(URI.class), any(Configuration.class))).thenAnswer(invocaton -> {
            fileSystem.initialize(invocaton.getArgument(0, URI.class), invocaton.getArgument(1, Configuration.class));
            return fileSystem;
        });
    }

    @Test
    public void testInconistentConfig1() throws Exception {
        // given
        Map<String, String> config = baseConfig();
        config.put(HdfsFileStorage.CONFIG_KERBEROS_PRINCIPAL, LOGIN_PRINCIPAL);

        // when

        // then
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> testSubject.init(config));
        assertEquals("hdfs.kerberos.keytab is needed when hdfs.kerberos.principal (== " + LOGIN_PRINCIPAL + ") is specified.", thrown.getMessage());
    }

    @Test
    public void testInconistentConfig2() throws Exception {
        // given
        Map<String, String> config = baseConfig();
        config.put(HdfsFileStorage.CONFIG_KERBEROS_KEYTAB, LOGIN_KEYTAB);

        // when
        testSubject = new HdfsFileStorage();

        //then
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> testSubject.init(config));
        assertEquals("hdfs.kerberos.principal is needed when hdfs.kerberos.keytab (== " + LOGIN_KEYTAB + ") is specified.", thrown.getMessage());
    }

    @Test
    public void testWithKerberos() throws Exception {
        // given
        Map<String, String> config = baseConfig();
        config.put(HdfsFileStorage.CONFIG_KERBEROS_PRINCIPAL, LOGIN_PRINCIPAL);
        config.put(HdfsFileStorage.CONFIG_KERBEROS_KEYTAB, LOGIN_KEYTAB);
        testSubject.init(config);

        // when
        doIo();

        // then
        verifyFileSystemCalls();
        verifyPrivilegedExecution();
    }

    @Test
    public void testWithoutKerberos() throws Exception {
        // given
        Map<String, String> config = baseConfig();
        testSubject.init(config);

        // when
        doIo();

        // then
        verifyFileSystemCalls();
        verifySimpleExecution();
    }

    private void verifyPrivilegedExecution() throws Exception {
        UserGroupInformation.loginUserFromKeytab(LOGIN_PRINCIPAL, LOGIN_KEYTAB);

        verify(userGroupInformation, never()).doAs(any(PrivilegedAction.class));
        verify(userGroupInformation, times(4)).doAs(any(PrivilegedExceptionAction.class));
    }

    private void verifySimpleExecution() throws Exception {
        userGroupInformationClass.verifyNoInteractions();

        UserGroupInformation.loginUserFromKeytab(anyString(), anyString());

        verify(userGroupInformation, never()).doAs(any(PrivilegedAction.class));
        verify(userGroupInformation, never()).doAs(any(PrivilegedExceptionAction.class));
    }

    private void verifyFileSystemCalls() throws IOException {
        FileSystem.get(any(URI.class), any(Configuration.class));

        Path path = new Path(ADJUSTED_DIRECTORY, JAR_FILE);
        List<String> expected = Lists.newArrayList(
                "create " + path,
                "open " + path,
                "getFileStatus " + path,
                "delete " + path);
        assertEquals(expected, fileSystem.getMethodCalls());

    }

    private void doIo() throws IOException {
        testSubject.upload(anInputStream(), JAR_FILE);
        testSubject.download(JAR_FILE);
        testSubject.exists(JAR_FILE);
        testSubject.delete(JAR_FILE);
    }

    private Map<String, String> baseConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(HdfsFileStorage.CONFIG_FSURL, FS_URL);
        config.put(HdfsFileStorage.CONFIG_DIRECTORY, DIRECTORY);
        return config;
    }

    private InputStream anInputStream() {
        return new ByteArrayInputStream(new byte[0]);
    }
}
