package org.apache.ranger.services.schemaregistry.client;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SchemaRegistryResourceMgr {

    private static final Logger LOG = Logger.getLogger(SchemaRegistryResourceMgr.class);

    private static final String SERDE = "serde";
    private static final String FILE = "file";
    private static final String SCHEMA_GROUP = "schema-group";
    private static final String SCHEMA_METADATA = "schema-metadata";
    private static final String SCHEMA_BRANCH = "schema-branch";
    private static final String SCHEMA_VERSION = "schema-version";

    private static final int LOOKUP_TIMEOUT_SEC = 5;


    public static List<String> getSchemaRegistryResources(String serviceName, Map<String, String> configs, ResourceLookupContext context) throws Exception {

        String userInput = context.getUserInput();
        String resource = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> resultList = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SchemaRegistryResourceMgr.getSchemaRegistryResources()  UserInput: \"" + userInput + "\" resource : " + resource + " resourceMap: " + resourceMap);
        }

        if (userInput != null
                && !userInput.isEmpty()
                && serviceName != null
                && resource != null
                && resourceMap != null
                && !resourceMap.isEmpty()) {
            //TODO: Add logging
            final RangerRegistryClient registryClient = SchemaRegistryConnectionMgr.getSchemaRegistryClient(serviceName, configs);
            if (registryClient != null) {
                Callable<List<String>> callableObj = null;
                try {
                    switch (resource.trim().toLowerCase()) {
                        case FILE: {
                            List<String> fileList = resourceMap.get(FILE);
                            // get the FileList for given Input
                            final String finalFileName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getFileList(finalFileName, fileList);
                                }
                            };
                            break;
                        }
                        case SERDE: {
                            List<String> schemaMeatadataList = resourceMap.get(SCHEMA_METADATA);
                            List<String> serdeList = resourceMap.get(SERDE);
                            // get the SerdeList for given Input
                            final String finalSerdeName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getSerdeList(finalSerdeName, schemaMeatadataList, serdeList);
                                }
                            };
                            break;
                        }
                        case SCHEMA_GROUP: {
                            List<String> schemaGroupList = resourceMap.get(SCHEMA_GROUP);
                            // get the SchemaGroupList for given Input
                            final String finalSchemaGroupName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getSchemaGroupList(finalSchemaGroupName, schemaGroupList);
                                }
                            };
                            break;
                        }
                        case SCHEMA_METADATA: {
                            List<String> schemaGroupList = resourceMap.get(SCHEMA_GROUP);
                            List<String> schemaMeatadataList = resourceMap.get(SCHEMA_METADATA);
                            // get the SchemaMetadataList for the given Input
                            final String finalSchemaName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getSchemaMetadataList(finalSchemaName, schemaGroupList, schemaMeatadataList);
                                }
                            };
                            break;
                        }
                        case SCHEMA_BRANCH: {
                            List<String> schemaGroupList = resourceMap.get(SCHEMA_GROUP);
                            List<String> schemaMeatadataList = resourceMap.get(SCHEMA_METADATA);
                            List<String> branchList = resourceMap.get(SCHEMA_BRANCH);
                            // get the SchemaBranchList for given Input
                            final String finalBranchName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getBranchList(finalBranchName, schemaGroupList, schemaMeatadataList, branchList);
                                }
                            };
                            break;
                        }
                        case SCHEMA_VERSION: {
                            List<String> schemaGroupList = resourceMap.get(SCHEMA_GROUP);
                            List<String> schemaList = resourceMap.get(SCHEMA_METADATA);
                            List<String> branchList = resourceMap.get(SCHEMA_BRANCH);
                            List<String> versionList = resourceMap.get(SCHEMA_VERSION);
                            // get the SchemaVersionList for given Input
                            final String finalVersionName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getVersionList(finalVersionName, schemaGroupList, schemaList, branchList, versionList);
                                }
                            };
                            break;
                        }
                        default:
                            break;

                    }
                } catch (Exception e) {
                    LOG.error("Unable to get hive resources.", e);
                    throw e;
                }
                if (callableObj != null) {
                    synchronized (registryClient) {
                        resultList = TimedEventUtil.timedTask(callableObj, LOOKUP_TIMEOUT_SEC,
                                TimeUnit.SECONDS);
                    }
                } else {
                    LOG.error("Could not initiate at timedTask");
                }
            }
        }

        return resultList;
    }
}
