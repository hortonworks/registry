/**
 * Copyright 2016-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.hortonworks.registries.auth.util;

import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRandomSignerSecretProvider {

    @Test
    public void testGetAndRollSecrets() throws Exception {
        long rolloverFrequency = 15 * 1000; // rollover every 15 sec
        // use the same seed so we can predict the RNG
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        byte[] secret1 = Long.toString(rand.nextLong()).getBytes();
        byte[] secret2 = Long.toString(rand.nextLong()).getBytes();
        byte[] secret3 = Long.toString(rand.nextLong()).getBytes();
        RandomSignerSecretProvider secretProvider =
                new RandomSignerSecretProvider(seed);
        try {
            secretProvider.init(null, null, rolloverFrequency);

            byte[] currentSecret = secretProvider.getCurrentSecret();
            byte[][] allSecrets = secretProvider.getAllSecrets();
            Assertions.assertArrayEquals(secret1, currentSecret);
            Assertions.assertEquals(2, allSecrets.length);
            Assertions.assertArrayEquals(secret1, allSecrets[0]);
            Assertions.assertNull(allSecrets[1]);
            Thread.sleep(rolloverFrequency + 2000);

            currentSecret = secretProvider.getCurrentSecret();
            allSecrets = secretProvider.getAllSecrets();
            Assertions.assertArrayEquals(secret2, currentSecret);
            Assertions.assertEquals(2, allSecrets.length);
            Assertions.assertArrayEquals(secret2, allSecrets[0]);
            Assertions.assertArrayEquals(secret1, allSecrets[1]);
            Thread.sleep(rolloverFrequency + 2000);

            currentSecret = secretProvider.getCurrentSecret();
            allSecrets = secretProvider.getAllSecrets();
            Assertions.assertArrayEquals(secret3, currentSecret);
            Assertions.assertEquals(2, allSecrets.length);
            Assertions.assertArrayEquals(secret3, allSecrets[0]);
            Assertions.assertArrayEquals(secret2, allSecrets[1]);
            Thread.sleep(rolloverFrequency + 2000);
        } finally {
            secretProvider.destroy();
        }
    }
}
