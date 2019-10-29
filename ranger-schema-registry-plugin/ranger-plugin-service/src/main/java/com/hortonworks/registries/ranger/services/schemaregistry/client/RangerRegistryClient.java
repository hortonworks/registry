package com.hortonworks.registries.ranger.services.schemaregistry.client;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import com.hortonworks.registries.ranger.services.schemaregistry.client.srclient.DefaultSRClient;
import com.hortonworks.registries.ranger.services.schemaregistry.client.srclient.SRClient;

import java.util.*;

public class RangerRegistryClient {
    private static final Logger LOG = Logger.getLogger(RangerRegistryClient.class);

    private SRClient srClient;
    private String serviceName;

    private static final String errMessage = " You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check server logs for more info.";

    public RangerRegistryClient(String serviceName, Map<String, String> configs) {
        this.serviceName = serviceName;
        initRegistryClient(configs);
    }

    private void initRegistryClient(Map<String, String> configs) {
        srClient = new DefaultSRClient(configs);
    }

    public HashMap<String, Object> connectionTest() {
        String errMsg = errMessage;
        HashMap<String, Object> responseData = new HashMap<String, Object>();

        try {
            srClient.testConnection();
            // If it doesn't throw exception, then assume the instance is
            // reachable
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg,
                    successMsg, null, null, responseData);
        } catch (Exception e) {
            LOG.error("Error connecting to SchemaRegistry. schemaRegistryClient=" + this, e);
            String failureMsg = "Unable to connect to SchemaRegistry instance."
                    + e.getMessage();
            BaseClient.generateResponseDataMap(false, failureMsg,
                    failureMsg + errMsg, null, null, responseData);
        }
        return responseData;
    }

    public List<String> getSchemaGroupList(String finalGroupName, List<String> groupList) {
        List<String> res = groupList;
        Collection<String> schemaGroups = srClient.getSchemaGroups();
        schemaGroups.forEach(gName -> {
            if (!res.contains(gName) && gName.contains(finalGroupName)) {
                res.add(gName);
            }
        });

        return res;
    }

    public List<String> getSchemaMetadataList(String finalSchemaMetadataName,
                                              List<String> schemaGroupList,
                                              List<String> schemaMetadataList) {
        List<String> res = schemaMetadataList;
        schemaGroupList.forEach(schemaGroupName -> {
            Collection<String> schemas = srClient.getSchemaNames(schemaGroupName);
            schemas.forEach(sName ->{
                if (!res.contains(sName) && sName.contains(finalSchemaMetadataName)) {
                    res.add(sName);
                }
            });

        });
        return res;
    }

    public List<String> getBranchList(String finalBranchName,
                                      List<String> schemaList,
                                      List<String> branchList) {
        List<String> res = branchList;
        schemaList.forEach(schemaMetadataName -> {
            Collection<String> branches = srClient.getSchemaBranches(schemaMetadataName);
            branches.forEach(bName -> {
                if (!res.contains(bName) && bName.contains(finalBranchName)) {
                    res.add(bName);
                }
            });
        });

        return res;
    }

    @Override
    public String toString() {
        return "ServiceKafkaClient [serviceName=" + serviceName + "]";
    }

}
