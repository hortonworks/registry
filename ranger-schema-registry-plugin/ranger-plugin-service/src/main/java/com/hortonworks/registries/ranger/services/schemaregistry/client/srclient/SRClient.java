package com.hortonworks.registries.ranger.services.schemaregistry.client.srclient;

import java.util.List;

public interface SRClient {
    List<String> getSchemaGroups();
    List<String> getSchemaNames(String schemaGroup);
    List<String> getSchemaBranches(String schemaMetadataName);
    void testConnection() throws Exception;
}

