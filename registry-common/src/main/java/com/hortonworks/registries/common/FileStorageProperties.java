/**
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.common;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FileStorageProperties {

    private String directory;
    private String fsUrl;
    private String kerberosPrincipal;
    private String keytabLocation;
    private String abfsImpl;
    private String abfssImpl;

    public FileStorageProperties() { }

    public FileStorageProperties(String directory, String fsUrl, String kerberosPrincipal, String keytabLocation, String abfsImpl, String abfssImpl) {
        this.directory = directory;
        this.fsUrl = fsUrl;
        this.kerberosPrincipal = kerberosPrincipal;
        this.keytabLocation = keytabLocation;
        this.abfsImpl = abfsImpl;
        this.abfssImpl = abfssImpl;
    }

    @JsonProperty
    public String getDirectory() {
        return directory;
    }

    @JsonProperty
    public void setDirectory(String directory) {
        this.directory = directory;
    }

    @JsonProperty
    public String getFsUrl() {
        return fsUrl;
    }

    @JsonProperty
    public void setFsUrl(String fsUrl) {
        this.fsUrl = fsUrl;
    }

    @JsonProperty("hdfs.kerberos.principal")
    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    @JsonProperty
    public void setKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
    }

    @JsonProperty("hdfs.kerberos.keytab")
    public String getKeytabLocation() {
        return keytabLocation;
    }

    @JsonProperty
    public void setKeytabLocation(String keytabLocation) {
        this.keytabLocation = keytabLocation;
    }

    @JsonProperty("fs.abfs.impl")
    public String getAbfsImpl() {
        return abfsImpl;
    }

    @JsonProperty
    public void setAbfsImpl(String abfsImpl) {
        this.abfsImpl = abfsImpl;
    }

    @JsonProperty("fs.abfss.impl")
    public String getAbfssImpl() {
        return abfssImpl;
    }

    @JsonProperty
    public void setAbfssImpl(String abfssImpl) {
        this.abfssImpl = abfssImpl;
    }
}
