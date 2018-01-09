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