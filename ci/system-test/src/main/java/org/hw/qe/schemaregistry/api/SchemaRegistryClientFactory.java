/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.hw.qe.schemaregistry.api.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.util.Strings;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Factory to choose schema registry.
 */
public final class SchemaRegistryClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryClientFactory.class);

  /**
   * Client types enum.
   */
  public enum SchemaClientType {
    REST, SCHEMA_REGISTRY_CLIENT
  }

  /**
   * Private constructor of schema client factory.
   */
  private SchemaRegistryClientFactory() {
  }

  /**
   * Get schema client.
   * @param schemaClientType Client type
   * @return Schema client to make rest calls
   */
  public static ISchemaRegistryClient getSchemaClient(SchemaClientType schemaClientType) {
    List<String> urlListForHA = Arrays.asList(Config.getRegistryURLs().split(","));
    List <URL> urlsForHA = new ArrayList<>();
    urlListForHA.forEach(url -> {
      try {
        urlsForHA.add(new URL(url));
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
    });
    URL activeURL = urlsForHA.get(0);
    String customSecurity = Config.getCustomSecurity();
    switch (schemaClientType) {
      case REST:
        LOG.error("Dont use rest unless you really need it");
        break;
      case SCHEMA_REGISTRY_CLIENT:
        HashMap<String, Serializable> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), activeURL.toString());
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(),
            60 * 60 * 1000L);
        if (!Strings.isNullOrEmpty(customSecurity)){
          HashMap<String, String> sslMap = new HashMap<>();
          sslMap.put("protocol", Config.getClientSSLProtocol());
          sslMap.put("trustStoreType", Config.getClientSSLTrustStoreType());
          sslMap.put("trustStorePath", Config.getClientSSLTrustStorePath());
          sslMap.put("trustStorePassword", Config.getClientSSLTrustStorePassword());
          config.put("schema.registry.client.ssl", sslMap);
        }
        return new SchemaRegistryClient(config);
      default:
        throw new RuntimeException("No client found for type " + schemaClientType.name());
    }
    throw new RuntimeException("No client found for type " + schemaClientType.name());
  }
}
