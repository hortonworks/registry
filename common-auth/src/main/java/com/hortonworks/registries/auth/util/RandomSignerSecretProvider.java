/**
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


import java.nio.charset.Charset;
import java.util.Random;

/**
 * A SignerSecretProvider that uses a random number as its secret.  It rolls
 * the secret at a regular interval.
 */
public class RandomSignerSecretProvider extends RolloverSignerSecretProvider {

    private final Random rand;

    public RandomSignerSecretProvider() {
        super();
        rand = new Random();
    }

    /**
     * This constructor lets you set the seed of the Random Number Generator and
     * is meant for testing.
     * @param seed the seed for the random number generator
     */
    public RandomSignerSecretProvider(long seed) {
        super();
        rand = new Random(seed);
    }

    @Override
    protected byte[] generateNewSecret() {
        return Long.toString(rand.nextLong()).getBytes(Charset.forName("UTF-8"));
    }
}
