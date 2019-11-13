/*
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
 */

package com.hortonworks.registries.storage.tool.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

public class Utils {

    @SuppressWarnings("unchecked")
    public static Map<String, Object> readConfig(String configFilePath) throws IOException {
        ObjectMapper objectMapper = new YAMLMapper();
        return objectMapper.readValue(new File(configFilePath), Map.class);
    }

    public static ClassLoader loadJarIntoClasspath(final File jarFile) throws Exception {
        try {
            final JarClassLoader customClassLoader = AccessController.doPrivileged((PrivilegedAction<JarClassLoader>)
                    () -> new JarClassLoader(new URL[0], Thread.currentThread().getContextClassLoader()));
            final URL url = jarFile.toURI().toURL();
            customClassLoader.addUrl(url);
            return customClassLoader;
        } catch (final Exception e) {
            System.out.println("Failed to load " + jarFile + " into classpath.");
            throw new Exception(e);
        }
    }

    static class JarClassLoader extends URLClassLoader {

        JarClassLoader(final URL[] urls, final ClassLoader parent) {
            super(urls, parent);
        }

        void addUrl(final URL url) {
            super.addURL(url);
        }
    }
}
