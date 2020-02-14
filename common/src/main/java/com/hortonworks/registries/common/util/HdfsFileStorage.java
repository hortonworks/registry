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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Set;


/**
 * HDFS based implementation for storing files.
 *
 */
public class HdfsFileStorage implements FileStorage {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsFileStorage.class);

    // the configuration keys
    public static final String CONFIG_FSURL = "fsUrl";
    public static final String CONFIG_DIRECTORY = "directory";
    public static final String CONFIG_KERBEROS_PRINCIPAL = "hdfs.kerberos.principal";
    public static final String CONFIG_KERBEROS_KEYTAB = "hdfs.kerberos.keytab";
    public static final Set<String> OWN_CONFIGS = ImmutableSet.of(CONFIG_FSURL, CONFIG_DIRECTORY, CONFIG_KERBEROS_PRINCIPAL, CONFIG_KERBEROS_KEYTAB);

    private String directory;
    private Configuration hdfsConfig;
    private URI fsUri;
    private boolean kerberos = false;

    @Override
    public void init(Map<String, String> config) throws IOException {
        String fsUrl = config.get(CONFIG_FSURL);
        String kerberosPrincipal = config.get(CONFIG_KERBEROS_PRINCIPAL);
        String keytabLocation = config.get(CONFIG_KERBEROS_KEYTAB);
        directory = config.getOrDefault(CONFIG_DIRECTORY, DEFAULT_DIR);

        hdfsConfig = new Configuration();

        for(Map.Entry<String, String> entry:
                Sets.filter(config.entrySet(), e -> !OWN_CONFIGS.contains(e.getKey()))) {
            hdfsConfig.set(entry.getKey(), entry.getValue());
        }

        // make sure fsUrl is set
        Preconditions.checkArgument(fsUrl != null, "fsUrl must be specified for HdfsFileStorage.");

        Preconditions.checkArgument((keytabLocation == null) == (kerberosPrincipal == null),
                "Both %s (== %s) and %s (== %s) must be specified to use kerberos.",
                CONFIG_KERBEROS_PRINCIPAL, kerberosPrincipal, CONFIG_KERBEROS_KEYTAB, keytabLocation);

        if (kerberosPrincipal != null) {
            LOG.info("Logging in as kerberos principal {}", kerberosPrincipal);
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, keytabLocation);
            kerberos = true;
        }

        directory = adjustDirectory(fsUrl, directory);
        fsUri = URI.create(fsUrl);

        LOG.info("Initialized with fsUrl={}, directory={}, kerberos principal={}", fsUrl, directory, kerberosPrincipal);
    }

    /**
     * Cloud storage filesystem url's usally contain paths. These should be prepended to the directory in order to work
     * properly.
     * @param fsUrl The HDFS or compatible filesystem URL
     * @param directory the directory to store hars
     * @return directory adjusted with path component in fsUrl
     */
    @VisibleForTesting
    static String adjustDirectory(String fsUrl, String directory) {
        String pathInFsUrl = URI.create(fsUrl).getPath();
        if ( !(pathInFsUrl.endsWith("/") || directory.startsWith("/")) ) {
            return pathInFsUrl + "/" + directory;
        }
        else if (pathInFsUrl.endsWith("/") && directory.startsWith("/")) {
            return pathInFsUrl + directory.substring(1);
        }
        else {
            return pathInFsUrl + directory;
        }
    }

    private boolean isKerberos() {
        return kerberos;
    }

    @Override
    public String upload(InputStream inputStream, String name) throws IOException {
        return isKerberos() ?
                doAsAndConvertException(() -> uploadInternal(inputStream, name)):
                uploadInternal(inputStream, name);
    }

    @Override
    public InputStream download(String name) throws IOException {
        return isKerberos() ?
                doAsAndConvertException(() -> downloadInternal(name)):
                downloadInternal(name);
    }

    @Override
    public boolean delete(String name) throws IOException {
        return isKerberos() ?
                doAsAndConvertException(() -> deleteInternal(name)):
                deleteInternal(name);
    }

    @Override
    public boolean exists(String name) {
        try {
            return isKerberos() ?
                    UserGroupInformation.getLoginUser().doAs((PrivilegedAction<Boolean>) () -> existsInternal(name)):
                    existsInternal(name);
        }
        catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private String uploadInternal(InputStream inputStream, String name) throws IOException {
        Path jarPath = new Path(directory, name);
        FileSystem fs = FileSystem.get(fsUri, hdfsConfig);
        try(FSDataOutputStream outputStream = fs.create(jarPath, false)) {
            ByteStreams.copy(inputStream, outputStream);
        }

        return jarPath.toString();
    }

    private InputStream downloadInternal(String name) throws IOException {
        Path filePath = new Path(directory, name);
        FileSystem fs = FileSystem.get(fsUri, hdfsConfig);
        return fs.open(filePath);
    }

    private boolean deleteInternal(String name) throws IOException {
        FileSystem fs = FileSystem.get(fsUri, hdfsConfig);
        return fs.delete(new Path(directory, name), true);
    }

    private boolean existsInternal(String name) {
        Path path = new Path(directory, name);
        try {
            FileSystem fs = FileSystem.get(fsUri, hdfsConfig);
            return fs.exists(path);
        } catch (Exception ex) {
            LOG.error("Exception occurred while calling exists(" + path + ")", ex);
        }
        return false;
    }


    private  <T> T doAsAndConvertException(PrivilegedExceptionAction<T> action) throws IOException {
        try {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            LOG.info("doAs, logged in user: {}", ugi);
            return ugi.doAs(action);
        }
        catch (IOException ioe) {
            throw ioe;
        }
        catch (InterruptedException ex) {
            throw new UncheckedIOException(new IOException(ex));
        }
    }
}