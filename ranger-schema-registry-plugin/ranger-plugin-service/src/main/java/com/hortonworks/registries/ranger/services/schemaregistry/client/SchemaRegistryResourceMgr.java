package com.hortonworks.registries.ranger.services.schemaregistry.client;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SchemaRegistryResourceMgr {

    private static final Logger LOG = Logger.getLogger(SchemaRegistryResourceMgr.class);

    private static final String SERDE = "serde";
    private static final String SCHEMA_GROUP = "schema-group";
    private static final String SCHEMA_METADATA = "schema-metadata";
    private static final String SCHEMA_BRANCH = "schema-branch";
    private static final String SCHEMA_VERSION = "schema-version";

    private static final List<String> asteriskList = Collections.singletonList("*");

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
                        case SCHEMA_GROUP: {
                            List<String> schemaGroupList = resourceMap.get(SCHEMA_GROUP);
                            // get the SchemaGroupList for given Input
                            final String finalSchemaGroupName = userInput;
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
                            final String finalSchemaName = userInput;
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getSchemaMetadataList(finalSchemaName, schemaGroupList, schemaMeatadataList);
                                }
                            };
                            break;
                        }
                        case SCHEMA_BRANCH: {
                            List<String> schemaMeatadataList = resourceMap.get(SCHEMA_METADATA);
                            List<String> branchList = resourceMap.get(SCHEMA_BRANCH);
                            // get the SchemaBranchList for given Input
                            final String finalBranchName = userInput;
                            callableObj = new Callable<List<String>>() {
                                @Override
                                public List<String> call() {
                                    return registryClient.getBranchList(finalBranchName, schemaMeatadataList, branchList);
                                }
                            };
                            break;
                        }
                        case SCHEMA_VERSION: case SERDE: {
                            return asteriskList;
                        }
                        default:
                            break;
                    }
                } catch (Exception e) {
                    LOG.error("Unable to get Schema Registry resources.", e);
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
