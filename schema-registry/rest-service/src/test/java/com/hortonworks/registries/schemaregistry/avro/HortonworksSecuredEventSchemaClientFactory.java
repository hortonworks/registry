package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;

public class HortonworksSecuredEventSchemaClientFactory {

    public static final String HTTP_BASIC_AUTHENTICATION_USERNAME = "schema.registry.client.http.basic.authentication.username";
    public static final String HTTP_BASIC_AUTHENTICATION_PASSWORD = "schema.registry.client.http.basic.authentication.password";
    public static final String SSL_CONFIG_PREFIX = "schema.registry.client.ssl";

    public SchemaRegistryClient createEventSchemaClient(Properties properties) {
        Map<String, Object> clientConf = new HashMap<>();

        if (properties.containsKey(SCHEMA_REGISTRY_URL.name()))
            clientConf.put(SCHEMA_REGISTRY_URL.name(), properties.getOrDefault(SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL.defaultValue()));

//        Map<String, Object> sslConf = new HashMap<>();
//        properties.stringPropertyNames().forEach(k -> {
//            if (k.startsWith(SSL_CONFIG_PREFIX))
//                sslConf.put(k.replace(SSL_CONFIG_PREFIX + ".", ""), properties.getProperty(k));
//        });
//        if (sslConf.size() > 0)
//            clientConf.put(SSL_CONFIG_PREFIX, sslConf);

        SchemaRegistryClient client = new SchemaRegistryClient(clientConf);

//        String username = (String) properties.get(HTTP_BASIC_AUTHENTICATION_USERNAME);
//        String password = (String) properties.get(HTTP_BASIC_AUTHENTICATION_PASSWORD);
//
//        if (username != null && password != null) {
//            HttpAuthenticationFeature httpBasicAuthFeature = HttpAuthenticationFeature.basicBuilder().credentials(username, password).build();
//
//            try {
//                Field f = SchemaRegistryClient.class.getDeclaredField("client");
//                f.setAccessible(true);
//                JerseyClient jerseyClient = (JerseyClient) f.get(client);
//                jerseyClient.register(httpBasicAuthFeature);
//            } catch (IllegalAccessException | NoSuchFieldException e) {
//                throw new RuntimeException(e);
//            }
//        }

        return client;
    }
}
