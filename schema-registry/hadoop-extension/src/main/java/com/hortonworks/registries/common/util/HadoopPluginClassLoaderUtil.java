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
package com.hortonworks.registries.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class HadoopPluginClassLoaderUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopPluginClassLoaderUtil.class);

    private static volatile HadoopPluginClassLoaderUtil config = null;
    private static String hadoopPluginLibDir = "hadoop-%-plugin-impl";

    public static HadoopPluginClassLoaderUtil getInstance() {
        HadoopPluginClassLoaderUtil result = config;
        if (result == null) {
            synchronized (HadoopPluginClassLoaderUtil.class) {
                result = config;
                if (result == null) {
                    config = result = new HadoopPluginClassLoaderUtil();
                }
            }
        }
        return result;
    }

    public URL[] getPluginFilesForServiceTypeAndPluginclass(String serviceType, Class<?> pluginClass) throws Exception {
        File pluginImplLibPath = getPluginImplLibPath(serviceType, pluginClass);
        return getPluginFiles(pluginImplLibPath);
    }

    private File getPluginImplLibPath(String serviceType, Class<?> pluginClass) throws URISyntaxException {
        URI uri = pluginClass.getProtectionDomain().getCodeSource().getLocation().toURI();
        String pluginParentDir = Paths.get(URI.create(uri.toString())).getParent().toString();

        return new File(pluginParentDir, hadoopPluginLibDir.replaceAll("%", serviceType));
    }

    private URL[] getPluginFiles(File libDir) {
        final List<URL> result = new ArrayList<>();

        File[] dirFiles = libDir.listFiles();
        if (dirFiles != null) {
            for (File dirFile : dirFiles) {
                try {
                    if (!dirFile.canRead()) {
                        LOG.error("{} is not readable!", dirFile.getAbsolutePath());
                    }
                    URL jarPath = dirFile.toURI().toURL();
                    LOG.info("Adding {}", dirFile.getAbsolutePath());
                    result.add(jarPath);
                } catch (Exception ex) {
                    LOG.warn("Failed to get URI for file {}", dirFile.getAbsolutePath(), ex);
                }
            }
        } else {
            LOG.warn("No files were found in the plugin dir {}", libDir);
        }

        return result.toArray(new URL[result.size()]);
    }
}
