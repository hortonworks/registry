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
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Entity;
import com.hortonworks.registries.shaded.javax.ws.rs.core.MultivaluedHashMap;
import com.hortonworks.registries.shaded.javax.ws.rs.core.MultivaluedMap;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Response;
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

  private final ObjectMapper objectMapper = new ObjectMapper();
  
  private String oauthServerUrl;
  private volatile String authToken;
  private HttpClientForOAuth2 httpClient;
  private String httpMethod;
  private String requestScope;

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

    params.put(OAuth2Config.OAUTH_HTTP_BASIC_USER, checkNotBlank(params.get(SchemaRegistryClient.Configuration.OAUTH_CLIENT_ID.name()), "clientId"));
    params.put(OAuth2Config.OAUTH_HTTP_BASIC_PASSWORD, checkNotBlank(params.get(SchemaRegistryClient.Configuration.OAUTH_CLIENT_SECRET.name()), "secret"));

    this.httpMethod = params.getOrDefault(SchemaRegistryClient.Configuration.OAUTH_REQUEST_METHOD.name(), "post");
    String scope = params.getOrDefault(SchemaRegistryClient.Configuration.OAUTH_SCOPE.name(), "");
    if (scope == null || scope.trim().equals("")) {
      requestScope = null;
    } else {
      requestScope = scope.trim();
    }
    this.oauthServerUrl = checkNotBlank(params.get(SchemaRegistryClient.Configuration.OAUTH_SERVER_URL.name()), "oauth2 url");
    this.httpClient = new HttpClientForOAuth2(params);
  }

  @Override
  public LoginContext login() throws LoginException {
    try {
      MultivaluedMap<String, String> reqData = new MultivaluedHashMap<>();
      reqData.add("grant_type", "client_credentials");
      if (requestScope != null) {
        reqData.add("scope", requestScope);
      }

      String responseAuthToken = httpClient.download(new URL(oauthServerUrl),
          httpMethod,
          Entity.form(reqData));

      if (responseAuthToken == null || responseAuthToken.trim().isEmpty()) {
        throw new LoginException("Empty response from the OAuth2 server.");
      } else {
        authToken = postProcessAuthToken(responseAuthToken);
      }
    } catch (Exception e) {
      LOG.error("Failed to retrieve OAuth2 token. ", e);
      throw new LoginException("Failed to retrieve OAuth2 token. " + e);
    }
    return null; //This is not used anywhere so we are returning null
  }

  @Override
  public <T> T doAction(PrivilegedAction<T> action) throws LoginException {
    T result;
    try {
      result = action.run();
    } catch (Exception e) {
      return renewAndRetry(action);
    }
    if (result instanceof Response) {
      Response response = (Response) result;
      if (response.getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()
          || response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()) {
        return renewAndRetry(action);
      }
    }
    return result;
  }

  @Override
  public void close() {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  private <T> T renewAndRetry(PrivilegedAction<T> action) throws LoginException {
    LOG.info("Request failed, renewing token...");
    login();
    return action.run();
  }

  /** Some servers return us an opaque token, others a JWT, while others return
   * a JSON containing a JWT. We will attempt to parse the token and extract the
   * JWT, if it's possible. If not, then we'll return the same input we received
   * from the server. */
  private String postProcessAuthToken(String authToken) {
    try {
      JsonNode json = objectMapper.readTree(authToken);
      if (json.has("access_token")) {
        return json.get("access_token").asText();
      }
    } catch (Exception ex) {
      LOG.debug("Failed to parse auth token: {}", ex.toString());
    }
    return authToken;
  }

  private static String checkNotBlank(String input, String err) {
    if (input == null || input.trim().equals("")) {
      throw new NullPointerException(err);
    }
    return input.trim();
  }
}
