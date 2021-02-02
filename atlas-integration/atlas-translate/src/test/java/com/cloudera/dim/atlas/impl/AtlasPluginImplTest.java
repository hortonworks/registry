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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AtlasPluginImplTest {

    private AtlasPluginImpl atlasPlugin;

    @BeforeEach
    public void setUp() {
        atlasPlugin = new AtlasPluginImpl();
    }

    @Test
    public void testWithRetry() {
        int num = atlasPlugin.withRetry(randomize -> 1, id -> getIfOddNumber(id));
        assertEquals(1, num);
    }

    @Test
    public void testWithRetryError() {
        assertThrows(IllegalArgumentException.class, () ->
            atlasPlugin.withRetry(randomize -> 2, id -> getIfOddNumber(id)),
                "Test should have failed in the previous step.");
    }

    private int getIfOddNumber(int number) {
        if (number % 2 == 1) {
            return number;
        }
        throw new IllegalArgumentException("Even number: " + number);
    }
}
