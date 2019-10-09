package org.apache.ranger.services.schemaregistry.client.srclient;

import java.util.List;
import java.util.Map;

public class DefaultSRClient implements SRClient {

    private Map<String, String> configs;

    public DefaultSRClient(Map<String, String> configs) {
    }

    @Override
    public List<String> getSchemaGroups() {
        return null;
    }

    @Override
    public List<String> getSchemaMetadataNames(String schemaGroup) {
        return null;
    }

    @Override
    public List<String> getSchemaBranches(String schemaGroupName, String schemaMetadataName) {
        return null;
    }

    @Override
    public List<String> getSchemaVersions(String schemaGroupName, String schemaMetadataName, String schemaBranchName) {
        return null;
    }

    @Override
    public List<String> getFiles() {
        return null;
    }

    @Override
    public List<String> getSerDes() {
        return null;
    }

    @Override
    public void testConnection() throws Exception {
        throw new Exception();
    }
}
