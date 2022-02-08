/*
 * Copyright 2016-2022 Cloudera, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.client;

import com.cloudera.dim.registry.oauth2.HttpClientForOAuth2;
import com.cloudera.dim.registry.oauth2.OAuth2Config;
import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Entity;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Form;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public class OAuth2Login implements Login {
  private static final Logger LOG = LoggerFactory.getLogger(OAuth2Login.class);
  
  private String oauthServerUrl;
  private String authToken;
  private HttpClientForOAuth2 httpClient;

  public String getAuthToken() {
    if (authToken == null) {
      throw new NullPointerException("Auth token is null. Please login first.");
    }
    return authToken;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, String loginContextName) {
    Map<String, String> params = new HashMap<>((Map<? extends String, ? extends String>) configs);
    
    params.put(OAuth2Config.OAUTH_HTTP_BASIC_USER, params.get(SchemaRegistryClient.Configuration.OAUTH_CLIENT_ID.name()));
    params.put(OAuth2Config.OAUTH_HTTP_BASIC_PASSWORD, params.get(SchemaRegistryClient.Configuration.OAUTH_CLIENT_SECRET.name()));
    this.oauthServerUrl = params.get(SchemaRegistryClient.Configuration.OAUTH_SERVER_URL.name());
    this.httpClient = new HttpClientForOAuth2(params);
  }

  @Override
  public LoginContext login() throws LoginException {
    try {
      authToken = httpClient.download(new URL(oauthServerUrl),
          "post",
          Entity.form(new Form("grant_type", "client_credentials")));
      if (authToken == null || authToken.trim().isEmpty()) {
        throw new LoginException("Empty response from the OAuth2 server.");
      }
    } catch (Exception e) {
      LOG.error("Failed to retrieve OAuth2 token. ", e);
      throw new LoginException("Failed to retrieve OAuth2 token. " + e);
    }
    return null; //This is not used anywhere so we are returning null
  }

  @Override
  public <T> T doAction(PrivilegedAction<T> action) throws LoginException {
    try {
      return action.run();
    } catch (Exception e) {
      LOG.info("Request failed, renewing token...");
      login();
      return action.run();
    }
  }

  @Override
  public void close() {
    if (httpClient != null) {
      httpClient.close();
    }
  }
}
