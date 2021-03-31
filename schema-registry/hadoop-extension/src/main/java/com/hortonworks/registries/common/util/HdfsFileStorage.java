/**
 * Copyright 2016-2021 Cloudera, Inc.
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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.hortonworks.registries.common.FileStorageConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * HDFS based implementation for storing files.
 */
public class HdfsFileStorage implements FileStorage {

    // the configuration keys
    public static final String CONFIG_FSURL = "fsUrl";
    public static final String CONFIG_DIRECTORY = "directory";
    public static final String CONFIG_KERBEROS_PRINCIPAL = "hdfs.kerberos.principal";
    public static final String CONFIG_KERBEROS_KEYTAB = "hdfs.kerberos.keytab";
    public static final Set<String> OWN_CONFIGS = ImmutableSet.of(CONFIG_FSURL, CONFIG_DIRECTORY, CONFIG_KERBEROS_PRINCIPAL, CONFIG_KERBEROS_KEYTAB);

    private final FileStorageConfiguration config;

    private final Supplier<HadoopPlugin> hdfsFilePlugin = Suppliers.memoize(this::getHdfsFilePlugin);

    public HdfsFileStorage(FileStorageConfiguration config) {
        this.config = checkNotNull(config, "config");
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

    /** This method should not be called directly. Use the Supplier instead. */
    private HadoopPlugin getHdfsFilePlugin() {
        return HadoopPluginFactory.createHdfsPlugin(config);
    }

    @Override
    public String upload(InputStream inputStream, String name) throws IOException {
        return hdfsFilePlugin.get().uploadInternal(inputStream, name);
    }

    @Override
    public InputStream download(String name) throws IOException {
        return hdfsFilePlugin.get().downloadInternal(name);
    }

    @Override
    public boolean delete(String name) throws IOException {
        return hdfsFilePlugin.get().deleteInternal(name);
    }

    @Override
    public boolean exists(String name) throws IOException {
        return hdfsFilePlugin.get().existsInternal(name);
    }



}