package org.apache.ranger.services.schemaregistry.client;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaRegistryClient {
    private static final Logger LOG = Logger.getLogger(SchemaRegistryClient.class);

    private enum RESOURCE_TYPE {
        SCHEMA
    }

    private String serviceName = null;
    private static final String errMessage = " You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check server logs for more info.";

    private static final String SCHEMA_KEY = "schema";
    private static final long LOOKUP_TIMEOUT_SEC = 5;

    public SchemaRegistryClient(String serviceName) {
        this.serviceName = serviceName;
    }

    public Map<String, Object> connectionTest() throws Exception {
        String errMsg = errMessage;
        Map<String, Object> responseData = new HashMap<String, Object>();
        try {
            getFileList(null, null);
            // If it doesn't throw exception, then assume the instance is
            // reachable
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg,
                    successMsg, null, null, responseData);
        } catch (Exception e) {
            LOG.error("Error connecting to SchemaRegistry. schemaRegistryClient=" + this, e);
            String failureMsg = "Unable to connect to Kafka instance."
                    + e.getMessage();
            BaseClient.generateResponseDataMap(false, failureMsg,
                    failureMsg + errMsg, null, null, responseData);
        }
        return responseData;
    }

    public List<String> getSerdeList(String finalSerdeName, List<String> serdeList) throws RuntimeException {
        throw new NotImplementedException();
    }

    public List<String> getFileList(String finalFileName, List<String> fileList) throws RuntimeException {
        throw new NotImplementedException();
    }

    public List<String> getSchemaList(String finalSchemaName, List<String> schemaList) throws RuntimeException {
        throw new NotImplementedException();
    }

    public List<String> getBranchList(String finalBranchName, List<String> schemaList, List<String> branchList) throws RuntimeException {
        throw new NotImplementedException();
    }

    public List<String> getVersionList(String finalVersionName, List<String> schemaList, List<String> branchList, List<String> versionList) throws RuntimeException {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        return "ServiceKafkaClient [serviceName=" + serviceName + "]";
    }
}
