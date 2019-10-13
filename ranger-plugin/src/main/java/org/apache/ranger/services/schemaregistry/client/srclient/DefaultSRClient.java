package org.apache.ranger.services.schemaregistry.client.srclient;

import com.hortonworks.registries.auth.KerberosLogin;
import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.auth.NOOPLogin;
import com.hortonworks.registries.schemaregistry.client.*;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;;
import java.io.UnsupportedEncodingException;
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
    private static final String REGISTY_CLIENT_JAAS_SECTION = "RegistryClient";
    private static Login login;
    private static final long KERBEROS_SYNCHRONIZATION_TIMEOUT_MS = 180000;

    static {
        String jaasConfigFile = System.getProperty("java.security.auth.login.config");
        if (jaasConfigFile != null && !jaasConfigFile.trim().isEmpty()) {
            KerberosLogin kerberosLogin = new KerberosLogin(KERBEROS_SYNCHRONIZATION_TIMEOUT_MS);
            kerberosLogin.configure(new HashMap<>(), REGISTY_CLIENT_JAAS_SECTION);
            try {
                kerberosLogin.login();
                login = kerberosLogin;
            } catch (LoginException e) {
                LOG.error("Could not login using jaas config  section " + REGISTY_CLIENT_JAAS_SECTION);
                login = new NOOPLogin();
            }
        } else {
            LOG.warn("System property for jaas config file is not defined. Its okay if schema registry is not running in secured mode");
            login = new NOOPLogin();
        }
    }

    private final Client client;
    private final UrlSelector urlSelector;
    private final Map<String, SchemaRegistryTargets> urlWithTargets;
    private final SchemaRegistryClient.Configuration configuration;

    public DefaultSRClient(Map<String, ?> conf) {
        configuration = new SchemaRegistryClient.Configuration(conf);
        ClientConfig config = createClientConfig(conf);
        ClientBuilder clientBuilder = JerseyClientBuilder.newBuilder()
                .withConfig(config)
                .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE);
        client = clientBuilder.build();
        client.register(MultiPartFeature.class);

        // get list of urls and create given or default UrlSelector.
        urlSelector = createUrlSelector();
        urlWithTargets = new ConcurrentHashMap<>();
    }

    private ClientConfig createClientConfig(Map<String, ?> conf) {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        config.property(ClientProperties.FOLLOW_REDIRECTS, true);
        for (Map.Entry<String, ?> entry : conf.entrySet()) {
            config.property(entry.getKey(), entry.getValue());
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
        private final WebTarget schemaRegistryVersion;
        private final WebTarget schemasTarget;

        SchemaRegistryTargets(WebTarget rootTarget) {
            schemaRegistryVersion = rootTarget.path(SCHEMA_REGISTRY_VERSION_PATH);
            schemasTarget = rootTarget.path(SCHEMAS_PATH);
        }
    }

    private SchemaRegistryTargets currentSchemaRegistryTargets() {
        String url = urlSelector.select();
        urlWithTargets.computeIfAbsent(url, s -> new SchemaRegistryTargets(client.target(s)));
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
        WebTarget webTarget = currentSchemaRegistryTargets().schemasTarget;
        try {
            String response = login.doAction(() ->
                    webTarget.request(MediaType.APPLICATION_JSON_TYPE).get(String.class));
            JSONArray mDataList = new JSONObject(response).getJSONArray("entities");
            mDataList.forEach(entity -> {
                JSONObject schemaMetadata = (JSONObject)((JSONObject)entity).get("schemaMetadata");
                String group = (String) schemaMetadata.get("schemaGroup");
                res.add(group);
            });
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public List<String> getSchemaMetadataNames(String schemaGroup) {
        ArrayList<String> res = new ArrayList<>();
        WebTarget webTarget = currentSchemaRegistryTargets().schemasTarget;
        try {
            String response = login.doAction(() ->
                    webTarget.request(MediaType.APPLICATION_JSON_TYPE).get(String.class));
            JSONArray mDataList = new JSONObject(response).getJSONArray("entities");
            mDataList.forEach(entity -> {
                JSONObject schemaMetadata = (JSONObject)((JSONObject)entity).get("schemaMetadata");
                String group = (String) schemaMetadata.get("schemaGroup");
                if(schemaGroup.matches(group)) {
                    String name = (String) schemaMetadata.get("name");
                    res.add(name);
                }
            });
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public List<String> getSchemaBranches(String schemaMetadataName) {
        ArrayList<String> res = new ArrayList<>();
        WebTarget target = currentSchemaRegistryTargets().schemasTarget.path(encode(schemaMetadataName) + "/branches");
        try {
            String response = login.doAction(() ->
                    target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class));
            JSONArray mDataList = new JSONObject(response).getJSONArray("entities");
            mDataList.forEach(entity -> {
                JSONObject branchInfo = (JSONObject)entity;
                String smName = (String) branchInfo.get("schemaMetadataName");
                if (smName.matches(schemaMetadataName)) {
                    String bName = (String) branchInfo.get("name");
                    res.add(bName);
                }

            });
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public List<String> getSchemaVersions(String schemaMetadataName, String schemaBranchName) {
        ArrayList<String> res = new ArrayList<>();
        WebTarget webTarget = currentSchemaRegistryTargets().schemasTarget.path(encode(schemaMetadataName) + "/versions").queryParam("branch", schemaBranchName);
        try {
            String response = login.doAction(() ->
                    webTarget.request(MediaType.APPLICATION_JSON_TYPE).get(String.class));
            JSONArray mDataList = new JSONObject(response).getJSONArray("entities");
            mDataList.forEach(entity -> {
                JSONObject versionInfo = (JSONObject)entity;
                res.add(versionInfo.get("version").toString());
            });
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public List<String> getFiles() {
        // Server API doesn't allow to get list of Files
        return new ArrayList<>();
    }

    @Override
    public List<String> getSerDes() {
        // Server API doesn't allow to get list of SerDes
        return new ArrayList<>();
    }

    @Override
    public void testConnection() throws Exception {
        WebTarget webTarget = currentSchemaRegistryTargets().schemaRegistryVersion;
        String responce = login.doAction(() ->
                webTarget.request(MediaType.APPLICATION_JSON_TYPE).get(String.class));
        if (!(responce.contains("version") && responce.contains("revision"))) {
            throw new Exception("Connection failed.");
        }
    }

    public static void main(String[] args) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://c7401:9090");
        SRClient client = new DefaultSRClient(conf);
        try {
            client.testConnection();
            System.out.println("OK");
            client.getSchemaGroups().forEach(System.out::println);
            client.getSchemaMetadataNames("Group1").forEach(System.out::println);
            client.getSchemaBranches("test1").forEach(System.out::println);
            client.getSchemaVersions("test1", "MASTER").forEach(System.out::println);
        } catch(Exception e) {
            System.out.println("FAIL");
        }
    }
}
