/**
 * Copyright 2017-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

public class ConnectionProperties {
    
    private String oracleNetSslVersion;
    
    private Boolean oracleNetSslServerDnMatch;
    
    private String trustStore;
    
    private String trustStoreType;
    
    private String keyStore;
    
    private String keyStoreType;

    @JsonProperty("oracle.net.ssl_version")
    public String getOracleNetSslVersion() {
        return oracleNetSslVersion;
    }
    
    @JsonProperty
    public void setOracleNetSslVersion(String oracleNetSslVersion) {
        this.oracleNetSslVersion = oracleNetSslVersion;
    }

    @JsonProperty("oracle.net.ssl_server_dn_match")
    public Boolean getOracleNetSslServerDnMatch() {
        return oracleNetSslServerDnMatch;
    }

    @JsonProperty
    public void setOracleNetSslServerDnMatch(Boolean oracleNetSslServerDnMatch) {
        this.oracleNetSslServerDnMatch = oracleNetSslServerDnMatch;
    }

    @JsonProperty("javax.net.ssl.trustStore")
    public String getTrustStore() {
        return trustStore;
    }

    @JsonProperty
    public void setTrustStore(String trustStore) {
        this.trustStore = trustStore;
    }

    @JsonProperty("javax.net.ssl.trustStoreType")
    public String getTrustStoreType() {
        return trustStoreType;
    }

    @JsonProperty
    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    @JsonProperty("javax.net.ssl.keyStore")
    public String getKeyStore() {
        return keyStore;
    }

    @JsonProperty
    public void setKeyStore(String keyStore) {
        this.keyStore = keyStore;
    }

    @JsonProperty("javax.net.ssl.keyStoreType")
    public String getKeyStoreType() {
        return keyStoreType;
    }

    @JsonProperty
    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }
    
    public boolean isEmpty() {
        return StringUtils.isBlank(oracleNetSslVersion) && StringUtils.isBlank(oracleNetSslServerDnMatch.toString()) && 
                StringUtils.isBlank(trustStore) && StringUtils.isBlank(trustStoreType) &&
                StringUtils.isBlank(keyStore) && StringUtils.isBlank(keyStoreType); 
    }
}

