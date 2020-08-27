/**
 * Copyright 2016-2020 Cloudera, Inc.
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
package com.cloudera.dim.atlas.shim;

import com.cloudera.dim.atlas.AtlasPlugin;
import com.cloudera.dim.atlas.AtlasUncheckedException;
import org.apache.atlas.plugin.classloader.AtlasPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;

/** Create instances of AtlasPlugin with dynamic classpath loading. */
public class AtlasPluginFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasPluginFactory.class);

    // the classpath loader will search for JAR files under the directory "atlas-[SERVICE_NAME]-plugin-impl"
    private static final String SERVICE_NAME = "schema-registry";
    private static final String ATLAS_PLUGIN_IMPL_CLASS = "com.cloudera.dim.atlas.impl.AtlasPluginImpl";

    private static class Proxy implements InvocationHandler {

        private final AtlasPluginClassLoader atlasPluginClassLoader;
        private final AtlasPlugin atlasPluginImpl;

        Proxy(Map<String, Object> config) {
            LOG.debug("==> Initialize AtlasPlugin");

            try {
                atlasPluginClassLoader = AtlasPluginClassLoader.getInstance(SERVICE_NAME, this.getClass());

                @SuppressWarnings("unchecked")
                Class<? extends AtlasPlugin> cls = (Class<? extends AtlasPlugin>) Class.forName(ATLAS_PLUGIN_IMPL_CLASS,
                        true, atlasPluginClassLoader);

                activatePluginClassLoader();

                atlasPluginImpl = cls.newInstance();
                atlasPluginImpl.initialize(config);
            } catch (Throwable excp) {
                // TODO CDPD-18839
                throw new RuntimeException("Error instantiating Atlas plugin implementation", excp);
            } finally {
                deactivatePluginClassLoader();
                LOG.debug("<== Initialize AtlasPlugin");
            }
        }

        private void activatePluginClassLoader() {
            if (atlasPluginClassLoader != null) {
                atlasPluginClassLoader.activate();
            }
        }

        private void deactivatePluginClassLoader() {
            if (atlasPluginClassLoader != null) {
                atlasPluginClassLoader.deactivate();
            }
        }

        /** Whenever a method of AtlasPlugin is invoked, the Atlas classpath is first loaded. This classpath
         * contains the classes necessary to send requests to Atlas. Once the response is received, the
         * classpath is unloaded.*/
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                activatePluginClassLoader();

                return method.invoke(atlasPluginImpl, args);
            } catch (UndeclaredThrowableException ex) {
                throw ex.getUndeclaredThrowable();
            } catch (AtlasUncheckedException ex) {
                if (ex.getCause() != null) {
                    throw ex.getCause();
                } else {
                    throw ex;
                }
            } catch (Error err) {
                // TODO CDPD-18839 We could use Thread.setDefaultUncaughtExceptionHandler();
                throw new AtlasUncheckedException("An error occurred while executing method \"" + method.getName() + "\" via the plugin.", err);
            } finally {
                deactivatePluginClassLoader();
            }
        }

    }

    /** Create an AtlasPlugin which will load the Atlas classpath for each method execution. */
    public static AtlasPlugin create(Map<String, Object> config) {
        Proxy atlasPlugin = new Proxy(config);

        return (AtlasPlugin) java.lang.reflect.Proxy.newProxyInstance(AtlasPluginFactory.class.getClassLoader(),
                new Class[] { AtlasPlugin.class }, atlasPlugin);
    }

    private AtlasPluginFactory() {}
}
