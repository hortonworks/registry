/**
 * Copyright 2017 Hortonworks.
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

package com.hortonworks.registries.schemaregistry.state.details;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.registries.schemaregistry.state.InbuiltSchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;

public class InitializedStateDetails implements AbstractStateDetails {

    public class MergedBranchDetails {
        @JsonProperty
        private String branchName;
        @JsonProperty
        private Long versionId;

        public MergedBranchDetails(String branchName, Long versionId) {
            this.branchName = branchName;
            this.versionId = versionId;
        }

        @JsonIgnore
        public String getBranchName() {
            return branchName;
        }

        @JsonIgnore
        public Long getVersionId() {
            return versionId;
        }
    }

    @JsonProperty
    Boolean isMergedFromBranch = false;

    @JsonProperty
    MergedBranchDetails mergedBranchDetails = null;

    public InitializedStateDetails() {

    }

    public InitializedStateDetails(String branchName, Long versionId) {
        isMergedFromBranch = true;
        mergedBranchDetails = new MergedBranchDetails(branchName, versionId);
    }

    @JsonIgnore
    public MergedBranchDetails getMergedBranchDetails() {
        return mergedBranchDetails;
    }

    @JsonIgnore
    @Override
    public InbuiltSchemaVersionLifecycleState getStateType() {
        return SchemaVersionLifecycleStates.INITIATED;
    }
}
