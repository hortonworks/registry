package org.apache.ranger.services.schemaregistry.client.srclient;

import java.util.List;

public interface SRClient {
    List<String> getSchemaGroups();
    List<String> getSchemaMetadataNames(String schemaGroup);
    List<String> getSchemaBranches(String schemaMetadataName);
    List<String> getSchemaVersions(String schemaMetadataName, String schemaBranchName);
    List<String> getFiles();
    List<String> getSerDes(String schemaMetadataName);
    void testConnection() throws Exception;
}

