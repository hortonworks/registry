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
package com.hortonworks.registries.auth.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuthenticationToken {

    @Test
    public void testAnonymous() {
        Assertions.assertNotNull(AuthenticationToken.ANONYMOUS);
        Assertions.assertEquals(null, AuthenticationToken.ANONYMOUS.getUserName());
        Assertions.assertEquals(null, AuthenticationToken.ANONYMOUS.getName());
        Assertions.assertEquals(null, AuthenticationToken.ANONYMOUS.getType());
        Assertions.assertEquals(-1, AuthenticationToken.ANONYMOUS.getExpires());
        Assertions.assertFalse(AuthenticationToken.ANONYMOUS.isExpired());
    }
}
