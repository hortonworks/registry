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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;

/** Classloader which loads the Hadoop dependencies. The jar files
 * need to be located in a directory named hadoop-schema-registry-plugin-impl */
public class HadoopPluginClassLoader extends URLClassLoader {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopPluginClassLoader.class);

    private static volatile HadoopPluginClassLoader instance = null;
    private static MyClassLoader primaryClassLoader = null;  // the main classloader of the service

    ThreadLocal<ClassLoader> preActivateClassLoader = new ThreadLocal<>();

    public HadoopPluginClassLoader(String pluginType, Class<?> pluginClass) throws Exception {
        super(HadoopPluginClassLoaderUtil.getInstance().getPluginFilesForServiceTypeAndPluginclass(pluginType, pluginClass), null);
        primaryClassLoader = AccessController.doPrivileged(new PrivilegedAction<MyClassLoader>() {
            @Override
            public MyClassLoader run() {
                return new MyClassLoader(Thread.currentThread().getContextClassLoader());
            }
        });
    }

    public static HadoopPluginClassLoader getInstance(String pluginType, Class<?> pluginClass) throws Exception {
        HadoopPluginClassLoader result = instance;
        if (result == null) {
            synchronized (HadoopPluginClassLoader.class) {
                result = instance;
                if (result == null && pluginClass != null) {
                    instance = result = AccessController.doPrivileged(
                            new PrivilegedExceptionAction<HadoopPluginClassLoader>() {
                                @Override
                                public HadoopPluginClassLoader run() throws Exception {
                                    return new HadoopPluginClassLoader(pluginType, pluginClass);
                                }
                            }
                    );
                }
            }
        }
        return result;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        Class<?> result = null;

        try {
            // first we try to find a class inside the plugin's classpath
            result = super.findClass(name);
        } catch (Throwable e) {
            // use the primary classpath to find the class
            MyClassLoader savedClassLoader = getPrimaryClassloader();
            if (savedClassLoader != null) {
                result = savedClassLoader.findClass(name);
            }
        }
        return result;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        Class<?> result = null;
        try {
            result = super.loadClass(name);
        } catch (Throwable e) {
            MyClassLoader savedClassLoader = getPrimaryClassloader();
            if (savedClassLoader != null) {
                result = savedClassLoader.loadClass(name);
            }
        }
        return result;
    }

    @Override
    public URL findResource(String name) {
        URL result = super.findResource(name);
        if (result == null) {
            MyClassLoader savedClassLoader = getPrimaryClassloader();
            if (savedClassLoader != null) {
                result = savedClassLoader.getResource(name);
            }
        }
        return result;
    }

    @Override
    public Enumeration<URL> findResources(String name) throws IOException {
        Enumeration<URL> result1 = null;
        Enumeration<URL> result2 = null;

        try {
            result1 = super.findResources(name);
        } catch (Throwable e) {
            // ignore it and try with the fallback classpath
        }
        MyClassLoader savedClassLoader = getPrimaryClassloader();
        if (savedClassLoader != null) {
            result2 = savedClassLoader.getResources(name);
        }

        return new MergeEnumeration(result1, result2);
    }

    public void activate() {
        LOG.debug("Activate HDFS plugin");
        preActivateClassLoader.set(Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(this);
    }

    public void deactivate() {
        ClassLoader classLoader = preActivateClassLoader.get();
        if (classLoader != null) {
            preActivateClassLoader.remove();
        } else {
            MyClassLoader savedClassLoader = getPrimaryClassloader();
            if (savedClassLoader != null && savedClassLoader.getParent() != null) {
                classLoader = savedClassLoader.getParent();
            }
        }

        if (classLoader != null) {
            Thread.currentThread().setContextClassLoader(classLoader);
        } else {
            LOG.warn("HdfsPluginClassLoader.deactivate() was not successful. Couldn't get the saved classLoader...");
        }

        LOG.debug("Deactivate HDFS plugin");
    }

    private MyClassLoader getPrimaryClassloader() {
        return primaryClassLoader;
    }

    static class MyClassLoader extends ClassLoader {

        public MyClassLoader(ClassLoader realClassLoader) {
            super(realClassLoader);
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            return super.findClass(name);
        }
    }

    static class MergeEnumeration implements Enumeration<URL> {

        private Enumeration<URL> e1;
        private Enumeration<URL> e2;

        public MergeEnumeration(Enumeration<URL> e1, Enumeration<URL> e2) {
            this.e1 = e1;
            this.e2 = e2;
        }

        @Override
        public boolean hasMoreElements() {
            return ((e1 != null && e1.hasMoreElements()) || (e2 != null && e2.hasMoreElements()));
        }

        @Override
        public URL nextElement() {
            if (e1 != null && e1.hasMoreElements()) {
                return e1.nextElement();
            } else if (e2 != null && e2.hasMoreElements()) {
                return e2.nextElement();
            }
            return null;
        }
    }
}
