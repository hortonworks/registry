package org.apache.ranger.services.schemaregistry.client;

import java.util.Map;

public class SchemaRegistryConnectionMgr {

    static public SchemaRegistryClient getSchemaRegistryClient(String serviceName,
                                                               Map<String, String> configs) throws Exception {
        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(
                    serviceName);
        return schemaRegistryClient;
    }

    /**
     * @param serviceName
     * @param configs
     * @return
     */
    public static Map<String, Object> connectionTest(String serviceName,
                                                     Map<String, String> configs) throws Exception {
        SchemaRegistryClient serviceKafkaClient = getSchemaRegistryClient(serviceName,
                configs);
        return serviceKafkaClient.connectionTest();
    }
}
