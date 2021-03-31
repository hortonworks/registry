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

import com.hortonworks.registries.common.FileStorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * Schema Registry is using Hadoop for various functions: checking keytabs, getting kerberos user groups
 * and also for storing files in HDFS/S3/ABFS.
 * <p>
 * Hadoop needs to live on a separate classpath in order to avoid classpath collision. This factory can be
 * used in multiple ways to gen instances of the Hadoop plugin. You may use it as a typical factory class.
 * It is also usable as a Provider, which may be injected with Guice.
 * </p>
 */
public class HadoopPluginFactory implements Provider<HadoopPlugin> {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopPluginFactory.class);

    // the classpath loader will search for JAR files under the directory "hadoop-[SERVICE_NAME]-plugin-impl"
    private static final String SERVICE_NAME = "schema-registry";
    private static final String HDFS_PLUGIN_IMPL_CLASS = "com.hortonworks.registries.common.hadoop.HdfsPluginImpl";
    private static final String KERBEROS_PLUGIN_IMPL_CLASS = "com.hortonworks.registries.common.hadoop.KerberosKeytabCheck";

    private static class Proxy implements InvocationHandler {

        private final HadoopPluginClassLoader hadoopPluginClassLoader;
        private final HadoopPlugin hdfsPluginImpl;

        Proxy(String pluginImplClass) {
            this(null, pluginImplClass);
        }

        Proxy(FileStorageConfiguration config, String pluginImplClass) {
            LOG.debug("==> Initialize HadoopPlugin for {}", pluginImplClass);

            try {
                hadoopPluginClassLoader = HadoopPluginClassLoader.getInstance(SERVICE_NAME, this.getClass());

                @SuppressWarnings("unchecked")
                Class<? extends HadoopPlugin> cls = (Class<? extends HadoopPlugin>) Class.forName(pluginImplClass,
                        true, hadoopPluginClassLoader);

                activatePluginClassLoader();

                hdfsPluginImpl = cls.newInstance();
                if (config != null) {
                    hdfsPluginImpl.initialize(config);
                }
            } catch (RuntimeException re) {
                throw re;
            } catch (Throwable excp) {
                throw new RuntimeException("Error instantiating HadoopPlugin implementation " + pluginImplClass, excp);
            } finally {
                deactivatePluginClassLoader();
                LOG.debug("<== Initialize HadoopPlugin");
            }
        }

        private void activatePluginClassLoader() {
            if (hadoopPluginClassLoader != null) {
                hadoopPluginClassLoader.activate();
            }
        }

        private void deactivatePluginClassLoader() {
            if (hadoopPluginClassLoader != null) {
                hadoopPluginClassLoader.deactivate();
            }
        }

        /** Whenever a method of HdfsFilePlugin is invoked, the Hadoop classpath is first loaded. This classpath
         * contains the classes necessary to send requests to Hadoop. Once the response is received, the
         * classpath is unloaded.*/
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                activatePluginClassLoader();

                return method.invoke(hdfsPluginImpl, args);
            } catch (UndeclaredThrowableException ex) {
                throw ex.getUndeclaredThrowable();
            } finally {
                deactivatePluginClassLoader();
            }
        }

    }

    /** Create an HdfsFilePlugin which will load the Hadoop classpath for each method execution. */
    public static HadoopPlugin createHdfsPlugin(FileStorageConfiguration config) {
        Proxy hdfsPlugin = new Proxy(config, HDFS_PLUGIN_IMPL_CLASS);

        return (HadoopPlugin) java.lang.reflect.Proxy.newProxyInstance(HadoopPluginFactory.class.getClassLoader(),
                new Class[] { HadoopPlugin.class }, hdfsPlugin);
    }

    public static HadoopPlugin createKeytabCheck() {
        Proxy kerberosPlugin = new Proxy(KERBEROS_PLUGIN_IMPL_CLASS);

        return (HadoopPlugin) java.lang.reflect.Proxy.newProxyInstance(HadoopPluginFactory.class.getClassLoader(),
                new Class[] { HadoopPlugin.class }, kerberosPlugin);
    }

    @Override
    public HadoopPlugin get() {
        return createKeytabCheck();
    }

}
