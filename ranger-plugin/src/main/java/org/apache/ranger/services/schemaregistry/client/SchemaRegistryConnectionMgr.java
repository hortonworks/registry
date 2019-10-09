package org.apache.ranger.services.schemaregistry.client;

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
    public static Map<String, Object> connectionTest(String serviceName,
                                                     Map<String, String> configs) throws Exception {
        RangerRegistryClient serviceKafkaClient = getSchemaRegistryClient(serviceName,
                configs);
        return serviceKafkaClient.connectionTest();
    }
}
