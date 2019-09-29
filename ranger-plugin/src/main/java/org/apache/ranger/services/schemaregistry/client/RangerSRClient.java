package org.apache.ranger.services.schemaregistry.client;

import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

public class RangerSRClient extends BaseClient {
    private static final Logger LOG = Logger.getLogger(RangerSRClient.class);

    private SchemaRegistryClient registryClient;

    private static final String errMessage = " You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check server logs for more info.";

    public RangerSRClient(String serviceName,  Map<String, String> configs) {
        super(serviceName, configs);
        initRegistryClient(configs);
    }

    private void initRegistryClient(Map<String, String> configs) {
        registryClient = new SchemaRegistryClient(configs);
    }

    public Map<String, Object> connectionTest() throws Exception {
        String errMsg = errMessage;
        Map<String, Object> responseData = new HashMap<String, Object>();
        try {
            getSchemaList(null, null);
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

    public List<String> getFileList(String finalFileName, List<String> fileList) throws RuntimeException {
        //TODO: Implementation should be added to SR client
        throw new NotImplementedException();
    }

    public List<String> getSchemaList(String finalSchemaName, List<String> schemaList) throws RuntimeException {
        //TODO: Implementation should be added to SR client
        return Arrays.asList("dummy-schema-1", "dummy-schema-2");
    }

    public List<String> getSerdeList(String finalSerdeName, List<String> schemaList, List<String> serdeList) throws RuntimeException {
        List<String> res = serdeList;
        schemaList.forEach(schemaName -> {
            Collection<SerDesInfo> serdes = registryClient.getSerDes(schemaName);
            serdes.forEach(s -> {
                String sName = s.getId().toString(); //TODO: Will we use ID?
                if (!res.contains(sName) && sName.matches(finalSerdeName)) {
                    res.add(sName);
                }
            });
        });

        return res;
    }

    public List<String> getBranchList(String finalBranchName, List<String> schemaList, List<String> branchList) {
        List<String> res = branchList;
        schemaList.forEach(schemaName -> {
            try {
                Collection<SchemaBranch> branches = registryClient.getSchemaBranches(schemaName);
                branches.forEach(b -> {
                    String bName = b.getName();
                    if (!res.contains(bName) && bName.matches(finalBranchName)) {
                        res.add(bName);
                    }
                });
            } catch (SchemaNotFoundException e) {
                // This exception should be ignored.
                // Recource policy can contain names of schemas that can be created in future.
            }
        });

        return res;
    }

    public List<String> getVersionList(String finalVersionName, List<String> schemaList, List<String> branchList, List<String> versionList) throws RuntimeException {
        List<String> res = versionList;
        schemaList.forEach(schemaName -> {
            branchList.forEach(branch -> {
                try {
                    Collection<SchemaVersionInfo> vList = registryClient.getAllVersions(branch, schemaName);
                    vList.forEach(v -> {
                        String vName = v.getName();
                        if (!res.contains(vName) && vName.matches(finalVersionName)) {
                            res.add(vName);
                        }
                    });
                } catch (SchemaNotFoundException e) {
                    // This exception should be ignored.
                    // Recource policy can contain names of schemas/branches that can be created in future.
                }
            });
        });

        return res;
    }

    @Override
    public String toString() {
        return "ServiceKafkaClient [serviceName=" + getSerivceName() + "]";
    }

}
