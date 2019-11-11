package com.hortonworks.registries.ranger.services.schemaregistry.client.srclient;

import com.hortonworks.registries.auth.KerberosLogin;
import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.auth.NOOPLogin;
import com.hortonworks.registries.schemaregistry.client.*;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.ClientProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;

import javax.net.ssl.*;
import javax.security.auth.login.LoginException;
import javax.ws.rs.core.MediaType;;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
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
    private static final String SSL_ALGORITHM = "TLS";
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
        final boolean SSLEnabled = false;
        if (SSLEnabled) {
            SSLContext ctx;
            try {
                ctx = createSSLContext(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            String commonNameForCertificate = (String)conf.get("commonNameForCertificate");
            HostnameVerifier hostnameVerifier = (String s, SSLSession sslSession) -> {
                return HttpsURLConnection
                        .getDefaultHostnameVerifier()
                        .verify(commonNameForCertificate != null ?
                                commonNameForCertificate : s, sslSession);

            };
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hostnameVerifier, ctx));
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

    private SSLContext createSSLContext(Map<String, ?> sslConfigurations) throws Exception {

        SSLContext context = SSLContext.getInstance(SSL_ALGORITHM);

        KeyManager[] km = null;

        String keyStorePath = (String)sslConfigurations.get("keyStorePath");
        String keyStorePassword = (String)sslConfigurations.get("keyStorePassword");
        Object obj = sslConfigurations.get("serverCertValidation");
        boolean serverCertValidation = (obj == null)? false : Boolean.getBoolean((String)sslConfigurations.get("serverCertValidation"));
        String trustStorePath = (String)sslConfigurations.get("trustStorePath");
        String trustStorePathPassword = (String)sslConfigurations.get("trustStorePathPassword");

        if (keyStorePassword != null) {
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

            InputStream in = null;

            in = getFileInputStream(keyStorePath);

            try {
                ks.load(in, keyStorePassword.toCharArray());
            } finally {
                if (in != null) {
                    in.close();
                }
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keyStorePassword.toCharArray());
            km = kmf.getKeyManagers();
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        TrustManager[] tm = null;

        if (serverCertValidation) {
            if (trustStorePath != null) {
                KeyStore trustStoreKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());

                InputStream in;

                in = getFileInputStream(trustStorePath);

                try {
                    trustStoreKeyStore.load(in, trustStorePathPassword.toCharArray());

                    trustManagerFactory.init(trustStoreKeyStore);

                    tm = trustManagerFactory.getTrustManagers();

                } finally {
                    if (in != null) {
                        in.close();
                    }
                }
            }
        } else {
            TrustManager ignoreValidationTM = new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    // Ignore Server Certificate Validation
                }

                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public void checkServerTrusted(X509Certificate[] chain,
                                               String authType)
                        throws CertificateException {
                    // Ignore Server Certificate Validation
                }
            };

            tm  = new TrustManager[] {ignoreValidationTM};
        }

        SecureRandom random = new SecureRandom();

        context.init(km, tm, random);

        return context;

    }

    private InputStream getFileInputStream(String path) throws FileNotFoundException {

        InputStream ret;

        File f = new File(path);

        if (f.exists()) {
            ret = new FileInputStream(f);
        } else {
            ret = getClass().getResourceAsStream(path);

            if (ret == null) {
                if (! path.startsWith("/")) {
                    ret = getClass().getResourceAsStream("/" + path);
                }
            }

            if (ret == null) {
                ret = ClassLoader.getSystemClassLoader().getResourceAsStream(path);
                if (ret == null) {
                    if (! path.startsWith("/")) {
                        ret = ClassLoader.getSystemResourceAsStream("/" + path);
                    }
                }
            }
        }

        return ret;
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
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://c7401:9090");
        SRClient client = new DefaultSRClient(conf);
        try {
            client.testConnection();
            System.out.println("OK");
            client.getSchemaGroups().forEach(System.out::println);
            client.getSchemaNames("Group1").forEach(System.out::println);
            client.getSchemaBranches("test1").forEach(System.out::println);
        } catch(Exception e) {
            System.out.println("FAIL");
        }
    }
}
