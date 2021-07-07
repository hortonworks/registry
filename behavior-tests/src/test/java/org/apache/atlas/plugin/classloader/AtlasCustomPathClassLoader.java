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
package org.apache.atlas.plugin.classloader;

import com.cloudera.dim.atlas.AtlasClasspathLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** We use a custom Atlas classpath loader because during testing we don't have the standard directory structure and
 * the default classpath loader cannot find the Atlas classes. The custom path is built for us by Gradle, which copies
 * the fatjar to a special place in the behavior-tests.
 *
 * @see <code>build.gradle</code>
 * @see <code>registry.ftl</code>
 */
public class AtlasCustomPathClassLoader implements AtlasClasspathLoaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasCustomPathClassLoader.class);

    private String customClasspath;

    public AtlasCustomPathClassLoader() { }

    @Override
    public void configure(Map<String, Object> config) {
        customClasspath = (String) config.get("customClasspath");
        LOG.info("Configured the Atlas classpath loader to load classes from {}", customClasspath);
    }

    @Override
    public AtlasPluginClassLoader create() {
        return new AtlasPluginClassLoader(customClasspath);
    }
}
