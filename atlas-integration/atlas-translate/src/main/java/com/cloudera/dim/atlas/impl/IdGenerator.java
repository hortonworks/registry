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
package com.cloudera.dim.atlas.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>Generate long ID numbers.</p>
 *
 * <p>The legacy Schema Registry used 64-bit Long values for storing
 * unique IDs. Atlas uses a 128-bit UUID for the same. Due to legacy
 * support we need to continue using the 64-bit ID. For this reason,
 * we always generate a unique ID for each entity in Atlas and store
 * them in a separate field.</p>
 */
public class IdGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(IdGenerator.class);

    private final AtomicLong id;

    public IdGenerator() {
        this(randomNumber());
    }

    public IdGenerator(long start) {
        this.id = new AtomicLong(start);
        LOG.trace("ID generator start number: {}", this.id.get());
    }

    private static long randomNumber() {
        // TODO Due to bug in Javascript we need to limit the range to [2^53, 2^53)
        // TODO CDPD-18844 See Jira for potential solutions.
        Integer intlimit = ThreadLocalRandom.current().nextInt();
        return intlimit.longValue();
    }

    /** Get the next ID value */
    public long nextId() {
        return id.getAndIncrement();
    }

    /** Randomize the value of the next ID */
    public IdGenerator randomize() {
        id.set(randomNumber());
        return this;
    }
}
