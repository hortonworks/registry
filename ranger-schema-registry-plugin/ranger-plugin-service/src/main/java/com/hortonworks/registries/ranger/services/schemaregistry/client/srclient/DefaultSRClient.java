package com.hortonworks.registries.ranger.services.schemaregistry.client.srclient;

import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.ranger.services.schemaregistry.client.srclient.util.SecurityUtils;
import com.hortonworks.registries.schemaregistry.client.*;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.client.ClientProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;

import javax.net.ssl.*;
import javax.ws.rs.core.MediaType;;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_CONNECTION_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_READ_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;

public class DefaultSRClient implements SRClient {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSRClient.class);

    private static final String SCHEMA_REGISTRY_PATH = "/api/v1/schemaregistry";
    private static final String SCHEMAS_PATH = SCHEMA_REGISTRY_PATH + "/schemas/";
    private static final String SCHEMA_REGISTRY_VERSION_PATH = SCHEMA_REGISTRY_PATH + "/version";
    private static final String SSL_ALGORITHM = "TLS";
    private final Client client;
    private final Login login;
    private final UrlSelector urlSelector;
    private final Map<String, SchemaRegistryTargets> urlWithTargets;
    private final SchemaRegistryClient.Configuration configuration;

    public DefaultSRClient(Map<String, ?> conf) {
        configuration = new SchemaRegistryClient.Configuration(conf);
        login = SecurityUtils.initializeSecurityContext(conf);
        ClientConfig config = createClientConfig(conf);
        final boolean SSLEnabled = SecurityUtils.isHttpsConnection(conf);
        if (SSLEnabled) {
            SSLContext ctx;
            try {
                ctx = SecurityUtils.createSSLContext(conf, SSL_ALGORITHM);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(HttpsURLConnection.getDefaultHostnameVerifier(), ctx));
        }
        client = Client.create(config);

        // get list of urls and create given or default UrlSelector.
        urlSelector = createUrlSelector();
        urlWithTargets = new ConcurrentHashMap<>();
    }

    private ClientConfig createClientConfig(Map<String, ?> conf) {
        ClientConfig config = new DefaultClientConfig();
        Map<String, Object> props = config.getProperties();
        props.put(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        props.put(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        props.put(ClientProperties.FOLLOW_REDIRECTS, true);
        for (Map.Entry<String, ?> entry : conf.entrySet()) {
            props.put(entry.getKey(), entry.getValue());
        }
        return config;
    }

    private UrlSelector createUrlSelector() {
        UrlSelector urlSelector = null;
        String rootCatalogURL = configuration.getValue(SCHEMA_REGISTRY_URL.name());
        String urlSelectorClass = configuration.getValue(SchemaRegistryClient.Configuration.URL_SELECTOR_CLASS.name());
        if (urlSelectorClass == null) {
            urlSelector = new LoadBalancedFailoverUrlSelector(rootCatalogURL);
        } else {
            try {
                urlSelector = (UrlSelector) Class.forName(urlSelectorClass)
                        .getConstructor(String.class)
                        .newInstance(rootCatalogURL);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        urlSelector.init(configuration.getConfig());

        return urlSelector;
    }

    private static class SchemaRegistryTargets {
        private final WebResource schemaRegistryVersion;
        private final WebResource schemasTarget;

        SchemaRegistryTargets(WebResource rootResource) {
            schemaRegistryVersion = rootResource.path(SCHEMA_REGISTRY_VERSION_PATH);
            schemasTarget = rootResource.path(SCHEMAS_PATH);
        }
    }

    private SchemaRegistryTargets currentSchemaRegistryTargets() {
        String url = urlSelector.select();
        urlWithTargets.computeIfAbsent(url, s -> new SchemaRegistryTargets(client.resource(s)));
        return urlWithTargets.get(url);
    }

    private static String encode(String schemaName) {
        try {
            return URLEncoder.encode(schemaName, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSchemaGroups() {
        ArrayList<String> res = new ArrayList<>();
        WebResource webResource = currentSchemaRegistryTargets().schemasTarget;
        try {
            String response = login.doAction(() ->
                    webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class));
            JSONArray mDataList = new JSONObject(response).getJSONArray("entities");
            int len = mDataList.length();
            for(int i = 0; i < len; i++) {
                JSONObject entity = mDataList.getJSONObject(i);
                JSONObject schemaMetadata = (JSONObject)entity.get("schemaMetadata");
                String group = (String) schemaMetadata.get("schemaGroup");
                res.add(group);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public List<String> getSchemaNames(String schemaGroup) {
        ArrayList<String> res = new ArrayList<>();
        WebResource webTarget = currentSchemaRegistryTargets().schemasTarget;
        try {
            String response = login.doAction(() ->
                    webTarget.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class));
            JSONArray mDataList = new JSONObject(response).getJSONArray("entities");
            int len = mDataList.length();
            for(int i = 0; i < len; i++) {
                JSONObject entity = mDataList.getJSONObject(i);
                JSONObject schemaMetadata = (JSONObject)entity.get("schemaMetadata");
                String group = (String) schemaMetadata.get("schemaGroup");
                if(schemaGroup.matches(group)) {
                    String name = (String) schemaMetadata.get("name");
                    res.add(name);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public List<String> getSchemaBranches(String schemaMetadataName) {
        ArrayList<String> res = new ArrayList<>();
        WebResource target = currentSchemaRegistryTargets().schemasTarget.path(encode(schemaMetadataName) + "/branches");
        try {
            String response = login.doAction(() ->
                    target.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class));
            JSONArray mDataList = new JSONObject(response).getJSONArray("entities");
            int len = mDataList.length();
            for(int i = 0; i < len; i++) {
                JSONObject entity = mDataList.getJSONObject(i);
                JSONObject branchInfo = entity;
                String smName = (String) branchInfo.get("schemaMetadataName");
                if (smName.matches(schemaMetadataName)) {
                    String bName = (String) branchInfo.get("name");
                    res.add(bName);
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public void testConnection() throws Exception {
        WebResource webTarget = currentSchemaRegistryTargets().schemaRegistryVersion;
        String responce = login.doAction(() ->
                webTarget.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class));
        if (!(responce.contains("version") && responce.contains("revision"))) {
            throw new Exception("Connection failed.");
        }
    }

    public static void main(String[] args) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "https://vomoshkovskyi-1.gce.cloudera.com:7790/");
        //conf.put(SCHEMA_REGISTRY_URL.name(), "https://c7401:8443");
        //conf.put("trustStorePath", "/home/vladimir/WorkCloudera/Repo/registry/ssl_trustore");
        //conf.put("trustStorePassword", "test12");
        conf.put("serverCertValidation", "false");
        System.setProperty("java.security.auth.login.config", "/home/vladimir/WorkCloudera/Repo/registry/jaas.conf");
        //conf.put("trustStoreType", "jks");
        //conf.put("commonNameForCertificate", "192.168.74.101,c7401");
        SRClient client = new DefaultSRClient(conf);
        try {
            client.testConnection();
            System.out.println("OK");
            client.getSchemaGroups().forEach(System.out::println);
            client.getSchemaNames("Group1").forEach(System.out::println);
            //client.getSchemaBranches("test1").forEach(System.out::println);
        } catch(Exception e) {
            System.out.println("FAIL");
        }
    }
}
