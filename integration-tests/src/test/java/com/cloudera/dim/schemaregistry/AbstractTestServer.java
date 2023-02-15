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
package com.cloudera.dim.schemaregistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractTestServer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTestServer.class);

    protected final AtomicBoolean running = new AtomicBoolean(false);
    protected final AtomicBoolean started = new AtomicBoolean(false);

    public AbstractTestServer() {
    }

    public boolean isRunning() {
        return running.get();
    }

    public abstract void start() throws Exception;

    public void stop() throws Exception {
        running.set(false);
        started.set(false);
    }

}
