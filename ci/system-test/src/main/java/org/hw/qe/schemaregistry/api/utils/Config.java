/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.utils;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Config object which reads from schema properties file.
 */
public final class Config {
  private static final Logger LOG = LoggerFactory.getLogger(Config.class);
  private static final String AUTOMATION_PROPERTIES = "schema-registry.properties";
  private static final CompositeConfiguration CONF_OBJ;

  /**
   * Private constructor to disallow creation of an objects.
   */
  private Config() {}

  static {
    CONF_OBJ = new CompositeConfiguration();
    LOG.info("Going to add properties from system properties.");
    CONF_OBJ.addConfiguration(new SystemConfiguration());

    LOG.info("Going to read properties from: " + AUTOMATION_PROPERTIES);
    final PropertiesConfiguration merlinConfig;
    try {
      merlinConfig = new PropertiesConfiguration(
          Config.class.getResource("/" + AUTOMATION_PROPERTIES));
      final FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
      reloadingStrategy.setRefreshDelay(2 * 60 * 1000);
      merlinConfig.setReloadingStrategy(reloadingStrategy);
      CONF_OBJ.addConfiguration(merlinConfig);
    } catch(ConfigurationException e) {
      LOG.error("Error reading properties :" + ExceptionUtils.getStackTrace(e));
    }
  }

  /**
   * Get ambari URL as saved in properties file.
   * @return Ambari URL string
   */
  public static String getRegistryURLs() {
    return CONF_OBJ.getString("registry.urls");
  }

  /**
   * Get custom security as saved in properties file.
   * @return security tool string
   */
  public static String getCustomSecurity() {
    return CONF_OBJ.getString("security");
  }

  /**
   * Get SSL protocol as saved in properties file.
   * @return SSL protocol string
   */
  public static String getClientSSLProtocol() {
    return CONF_OBJ.getString("schema.registry.client.ssl.protocol");
  }

  /**
   * Get Client SSL Trust Store Type as saved in properties file.
   * @return Client SSL Trust Store Type string
   */
  public static String getClientSSLTrustStoreType() {
    return CONF_OBJ.getString("schema.registry.client.ssl.trustStoreType");
  }

  /**
   * Get Client SSL Trust Store Path as saved in properties file.
   * @return Client SSL Trust Store Path string
   */
  public static String getClientSSLTrustStorePath() {
    return CONF_OBJ.getString("schema.registry.client.ssl.trustStorePath");
  }

  /**
   * Get Client SSL Trust Store Password as saved in properties file.
   * @return Client SSL Trust Store Password string
   */
  public static String getClientSSLTrustStorePassword() {
    return CONF_OBJ.getString("schema.registry.client.ssl.trustStorePassword");
  }
}
