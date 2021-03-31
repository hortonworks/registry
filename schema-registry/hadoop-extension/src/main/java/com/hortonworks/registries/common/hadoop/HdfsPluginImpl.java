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
package com.hortonworks.registries.common.hadoop;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.common.util.HdfsFileStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.hortonworks.registries.common.util.HdfsFileStorage.CONFIG_DIRECTORY;
import static com.hortonworks.registries.common.util.HdfsFileStorage.CONFIG_FSURL;
import static com.hortonworks.registries.common.util.HdfsFileStorage.CONFIG_KERBEROS_KEYTAB;
import static com.hortonworks.registries.common.util.HdfsFileStorage.CONFIG_KERBEROS_PRINCIPAL;
import static com.hortonworks.registries.common.util.HdfsFileStorage.OWN_CONFIGS;

/** This class is the implementation of the HDFS FileStorage plugin. It uses Hadoop classes
 * and therefore needs to be loaded on a separate classpath in order to not interfere with
 * the main classpath of Schema Registry. Please see readme.md of the subproject for more
 * information. */
public class HdfsPluginImpl extends AbstractHadoopPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsFileStorage.class);

    private String directory;
    private Configuration hdfsConfig;
    private URI fsUri;
    private boolean kerberosEnabled = false;

    public HdfsPluginImpl() { }

    @Override
    public void initialize(FileStorageConfiguration config) throws IOException {
        final Map<String, String> props = config.getProperties();
        String fsUrl = props.get(CONFIG_FSURL);
        String kerberosPrincipal = props.get(CONFIG_KERBEROS_PRINCIPAL);
        String keytabLocation = props.get(CONFIG_KERBEROS_KEYTAB);
        directory = props.getOrDefault(CONFIG_DIRECTORY, FileStorage.DEFAULT_DIR);

        hdfsConfig = new Configuration();

        for (Map.Entry<String, String> entry: Sets.filter(props.entrySet(), e -> !OWN_CONFIGS.contains(e.getKey()))) {
            hdfsConfig.set(entry.getKey(), entry.getValue());
        }

        // make sure fsUrl is set
        checkArgument(fsUrl != null, "fsUrl must be specified for HdfsFileStorage.");

        checkArgument(keytabLocation != null || kerberosPrincipal == null,
                "%s is needed when %s (== %s) is specified.",
                CONFIG_KERBEROS_KEYTAB, CONFIG_KERBEROS_PRINCIPAL, kerberosPrincipal);

        checkArgument(kerberosPrincipal != null || keytabLocation == null,
                "%s is needed when %s (== %s) is specified.",
                CONFIG_KERBEROS_PRINCIPAL, CONFIG_KERBEROS_KEYTAB, keytabLocation);

        if (kerberosPrincipal != null) {
            LOG.info("Logging in as kerberos principal {}", kerberosPrincipal);
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, keytabLocation);
            kerberosEnabled = true;
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
        if (!(pathInFsUrl.endsWith("/") || directory.startsWith("/"))) {
            return pathInFsUrl + "/" + directory;
        } else if (pathInFsUrl.endsWith("/") && directory.startsWith("/")) {
            return pathInFsUrl + directory.substring(1);
        } else {
            return pathInFsUrl + directory;
        }
    }

    private boolean isKerberosEnabled() {
        return kerberosEnabled;
    }

    @Override
    public String uploadInternal(InputStream inputStream, String name) throws IOException {
        return execute(() -> {
            Path jarPath = new Path(directory, name);
            try (FSDataOutputStream outputStream = getFileSystem().create(jarPath, false)) {
                ByteStreams.copy(inputStream, outputStream);
            }

            return jarPath.toString();
        });
    }

    @Override
    public InputStream downloadInternal(String name) throws IOException {
        return execute(() -> {
            Path filePath = new Path(directory, name);
            return getFileSystem().open(filePath);
        });
    }

    @Override
    public boolean deleteInternal(String name) throws IOException {
        return execute(() -> getFileSystem().delete(new Path(directory, name), true));
    }

    @Override
    public boolean existsInternal(String name) throws IOException {
        return execute(() -> {
            Path path = new Path(directory, name);
            return getFileSystem().exists(path);
        });
    }

    private FileSystem getFileSystem() throws IOException {
        return FileSystem.get(fsUri, hdfsConfig);
    }

    private  <T> T execute(PrivilegedExceptionAction<T> action) throws IOException {
        try {
            if (isKerberosEnabled()) {
                UserGroupInformation ugi = UserGroupInformation.getLoginUser();
                LOG.info("doAs, logged in user: {}", ugi);
                return ugi.doAs(action);
            } else {
                return action.run();
            }
        } catch (IOException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

}
