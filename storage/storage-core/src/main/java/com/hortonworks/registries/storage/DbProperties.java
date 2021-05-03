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

public class DbProperties {
    private String dataSourceClassName;
    private String dataSourceUrl;
    private String dataSourceUser;
    private String dataSourcePassword;
    public ConnectionProperties connectionProperties;

    @JsonProperty
    public String getDataSourceClassName() {
        return dataSourceClassName;
    }

    @JsonProperty
    public void setDataSourceClassName(String dataSourceClassName) {
        this.dataSourceClassName = dataSourceClassName;
    }

    @JsonProperty("dataSource.url")
    public String getDataSourceUrl() {
        return dataSourceUrl;
    }

    @JsonProperty
    public void setDataSourceUrl(String dataSourceUrl) {
        this.dataSourceUrl = dataSourceUrl;
    }

    @JsonProperty("dataSource.user")
    public String getDataSourceUser() {
        return dataSourceUser;
    }

    @JsonProperty
    public void setDataSourceUser(String dataSourceuser) {
        this.dataSourceUser = dataSourceuser;
    }

    @JsonProperty("dataSource.password")
    public String getDataSourcePassword() {
        return dataSourcePassword;
    }

    @JsonProperty
    public void setDataSourcePassword(String dataSourcepassword) {
        this.dataSourcePassword = dataSourcepassword;
    }

    @JsonProperty("connection.properties")
    public ConnectionProperties getConnectionProperties() {
        return connectionProperties;
    }

    @JsonProperty
    public void setConnectionProperties(ConnectionProperties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }
    
    public boolean isEmpty() {
        return StringUtils.isBlank(dataSourceClassName) && StringUtils.isBlank(dataSourceUrl) && StringUtils.isBlank(dataSourceUser) 
                && StringUtils.isBlank(dataSourcePassword) && connectionProperties.isEmpty();
    }

    public DbProperties createCopyWithoutConnectionProperties(DbProperties other) {
        DbProperties result = new DbProperties();
        result.setDataSourceClassName(other.getDataSourceClassName());
        result.setDataSourceUrl(other.getDataSourceUrl());
        result.setDataSourceUser(other.getDataSourceUser());
        result.setDataSourcePassword(other.getDataSourcePassword());
        return result;
    }
}
