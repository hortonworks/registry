/*
 * Copyright 2016-2020 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.authorizer;

import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.agent.util.TestAuthorizer;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthorizerFactoryTest {

    @Test
    public void getAuthorizer() {

        Map<String, Object> props3 = new HashMap<>();
        props3.put(Authorizer.AUTHORIZER_CONFIG, TestAuthorizer.class.getCanonicalName());
        Authorizer authorizer3 = AuthorizerFactory.getAuthorizer(props3);
        assertTrue(authorizer3 instanceof TestAuthorizer);

    }

}
