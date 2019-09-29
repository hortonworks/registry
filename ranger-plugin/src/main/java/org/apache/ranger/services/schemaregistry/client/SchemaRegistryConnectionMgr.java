package org.apache.ranger.services.schemaregistry.client;

import java.util.Map;

public class SchemaRegistryConnectionMgr {

    static public RangerSRClient getSchemaRegistryClient(String serviceName,
                                                         Map<String, String> configs) {
        RangerSRClient rangerSRClient = new RangerSRClient(serviceName, configs);
        return rangerSRClient;
    }

    /**
     * @param serviceName
     * @param configs
     * @return
     */
    public static Map<String, Object> connectionTest(String serviceName,
                                                     Map<String, String> configs) throws Exception {
        RangerSRClient serviceKafkaClient = getSchemaRegistryClient(serviceName,
                configs);
        return serviceKafkaClient.connectionTest();
    }
}
