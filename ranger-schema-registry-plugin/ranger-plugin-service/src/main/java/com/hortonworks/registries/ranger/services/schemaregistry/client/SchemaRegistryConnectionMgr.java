package com.hortonworks.registries.ranger.services.schemaregistry.client;

import com.hortonworks.registries.ranger.services.schemaregistry.client.RangerRegistryClient;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryConnectionMgr {

    static public RangerRegistryClient getSchemaRegistryClient(String serviceName,
                                                               Map<String, String> configs) {
        RangerRegistryClient rangerRegistryClient = new RangerRegistryClient(serviceName, configs);
        return rangerRegistryClient;
    }

    /**
     * @param serviceName
     * @param configs
     * @return
     */
    public static HashMap<String, Object> connectionTest(String serviceName,
                                                         Map<String, String> configs) throws Exception {
        RangerRegistryClient serviceSRClient = getSchemaRegistryClient(serviceName,
                configs);
        return serviceSRClient.connectionTest();
    }
}
