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
package com.cloudera.dim.registry.oauth2.variant;

import com.hortonworks.registries.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.core.JsonParser;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.core.TreeNode;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.DeserializationContext;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.JsonDeserializer;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;

/** Based on https://datatracker.ietf.org/doc/html/rfc7517 */
/*
  {"keys":
       [
         {"kty":"EC",
          "crv":"P-256",
          "x":"MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4",
          "y":"4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM",
          "use":"enc",
          "kid":"1"},

         {"kty":"RSA",
          "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx
     4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMs
     tn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2
     QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbI
     SD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqb
     w0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
          "e":"AQAB",
          "alg":"RS256",
          "kid":"2011-04-29"}
       ]
     }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class Jwk {

    // https://www.rfc-editor.org/rfc/rfc7518.html#page-28
    private String kty;
    private String crv;
    private String x;
    private String y;
    private String n;
    private String e;
    // https://connect2id.com/products/nimbus-jose-jwt/examples/jwk-generation
    private String k;
    private String use;
    private String alg;
    private String kid;
    // x5c is sometimes a String and sometimes an array of strings, so we need a custom serializer
    private X5c x5c;

    public Jwk() { }

    public String getKty() {
        return kty;
    }

    public void setKty(String kty) {
        this.kty = kty;
    }

    public String getCrv() {
        return crv;
    }

    public void setCrv(String crv) {
        this.crv = crv;
    }

    public String getX() {
        return x;
    }

    public void setX(String x) {
        this.x = x;
    }

    public String getY() {
        return y;
    }

    public void setY(String y) {
        this.y = y;
    }

    public String getN() {
        return n;
    }

    public void setN(String n) {
        this.n = n;
    }

    public String getE() {
        return e;
    }

    public void setE(String e) {
        this.e = e;
    }

    public String getUse() {
        return use;
    }

    public void setUse(String use) {
        this.use = use;
    }

    public String getAlg() {
        return alg;
    }

    public void setAlg(String alg) {
        this.alg = alg;
    }

    public String getKid() {
        return kid;
    }

    public void setKid(String kid) {
        this.kid = kid;
    }

    public X5c getX5c() {
        return x5c;
    }

    public void setX5c(X5c x5c) {
        this.x5c = x5c;
    }

    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    @JsonDeserialize(using = X5cDeserializer.class)
    public static class X5c {
        private String value;

        public X5c() { }

        public X5c(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static class X5cDeserializer extends JsonDeserializer<X5c> {

        @Override
        public X5c deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
            TreeNode root = mapper.readTree(jsonParser);
            if (root == null) {
                return null;
            }
            if (root.isArray()) {
                if (root.size() <= 0) {
                    return null;
                } else {
                    String[] values = mapper.readerForArrayOf(String.class).readValue(root.traverse());
                    return new X5c(values[0]);
                }
            } else {
                return new X5c(mapper.readValue(jsonParser, String.class));
            }
        }
    }
}
