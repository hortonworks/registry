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

    private static final String  SERDE = "serde";
    private static final String  FILE = "file";
    private static final String  SCHEMA = "schema";
    private static final String  BRANCH = "branch";
    private static final String  VERSION = "schema-version";

    private static final int TIMEOUT = 5; // Seconds


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
            final SchemaRegistryClient registryClient = SchemaRegistryConnectionMgr.getSchemaRegistryClient(serviceName, configs);
            if (registryClient != null) {
                Callable<List<String>> callableObj = null;
                try {
                    switch (resource.trim().toLowerCase()) {
                        case SERDE: {
                            List<String> serdeList = resourceMap.get(SERDE);
                            // get the SerdeList for given Input
                            final String finalSerdeName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getSerdeList(finalSerdeName, serdeList);
                                }
                            };
                            break;
                        }
                        case FILE: {
                            List<String> fileList = resourceMap.get(FILE);
                            // get the SerdeList for given Input
                            final String finalFileName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getFileList(finalFileName, fileList);
                                }
                            };
                            break;
                        }
                        case SCHEMA: {
                            List<String> schemaList = resourceMap.get(SCHEMA);
                            // get the SerdeList for given Input
                            final String finalSchemaName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getSchemaList(finalSchemaName, schemaList);
                                }
                            };
                            break;
                        }
                        case BRANCH: {
                            List<String> schemaList = resourceMap.get(SCHEMA);
                            List<String> branchList = resourceMap.get(BRANCH);
                            // get the SerdeList for given Input
                            final String finalBranchName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getBranchList(finalBranchName, schemaList, branchList);
                                }
                            };
                            break;
                        }
                        case VERSION: {
                            List<String> schemaList = resourceMap.get(SCHEMA);
                            List<String> branchList = resourceMap.get(BRANCH);
                            List<String> versionList = resourceMap.get(VERSION);
                            // get the SerdeList for given Input
                            final String finalVersionName = userInput + "*";
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getVersionList(finalVersionName, schemaList, branchList, versionList);
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
                        resultList = TimedEventUtil.timedTask(callableObj, TIMEOUT,
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
