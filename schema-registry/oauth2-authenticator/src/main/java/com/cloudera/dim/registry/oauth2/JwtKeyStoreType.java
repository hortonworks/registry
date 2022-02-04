/**
 * Copyright 2016-2022 Cloudera, Inc.
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
package com.cloudera.dim.registry.oauth2;

public enum JwtKeyStoreType {
    URL("url"),
    PROPERTY("property"),
    KEYSTORE("keystore"),
    JWK("jwk");

    private final String value;

    JwtKeyStoreType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static JwtKeyStoreType parseString(String str) {
        if (str == null) {
            return null;
        }
        for (JwtKeyStoreType pkst : values()) {
            if (pkst.getValue().equals(str)) {
                return pkst;
            }
        }

        throw new IllegalArgumentException("Unknown public key store type: " + str);
    }
}
