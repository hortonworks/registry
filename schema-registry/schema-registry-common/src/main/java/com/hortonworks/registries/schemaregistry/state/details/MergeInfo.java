/**
 * Copyright 2017-2021 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.state.details;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MergeInfo {
    @JsonProperty
    private String schemaBranchName;
    @JsonProperty
    private Long schemaVersionId;

    private MergeInfo() {

    }

    public MergeInfo(String branchName, Long versionId) {
        this.schemaBranchName = branchName;
        this.schemaVersionId = versionId;
    }

    @JsonIgnore
    public String getSchemaBranchName() {
        return schemaBranchName;
    }

    @JsonIgnore
    public Long getSchemaVersionId() {
        return schemaVersionId;
    }
}