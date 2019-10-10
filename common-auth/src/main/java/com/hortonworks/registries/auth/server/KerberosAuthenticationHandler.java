/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.hortonworks.registries.auth.server;

import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.client.KerberosAuthenticator;
import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.auth.util.KerberosUtil;
import com.hortonworks.registries.auth.util.Utils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hortonworks.registries.auth.PlatformName.IBM_JAVA;

/**
 * The {@link KerberosAuthenticationHandler} implements the Kerberos SPNEGO authentication mechanism for HTTP.
 * <p>
 * The supported configuration properties are:
 * <ul>
 * <li>kerberos.principal: the Kerberos principal to used by the server. As stated by the Kerberos SPNEGO
 * specification, it should be <code>HTTP/${HOSTNAME}@{REALM}</code>. The realm can be omitted from the
 * principal as the JDK GSS libraries will use the realm name of the configured default realm.
 * It does not have a default value.</li>
 * <li>kerberos.keytab: the keytab file containing the credentials for the Kerberos principal.
 * It does not have a default value.</li>
 * <li>kerberos.name.rules: kerberos names rules to resolve principal names, see 
 * {@link KerberosName#setRules(String)}</li>
 * </ul>
 */
public class KerberosAuthenticationHandler implements AuthenticationHandler {
    private static Logger LOG = LoggerFactory.getLogger(KerberosAuthenticationHandler.class);

    /**
     * Kerberos context configuration for the JDK GSS library.
     */
    private static class KerberosConfiguration extends Configuration {
        private String keytab;
        private String principal;

        public KerberosConfiguration(String keytab, String principal) {
            this.keytab = keytab;
            this.principal = principal;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<String, String>();
            if (IBM_JAVA) {
                options.put("useKeytab",
                        keytab.startsWith("file://") ? keytab : "file://" + keytab);
                options.put("principal", principal);
                options.put("credsType", "acceptor");
            } else {
                options.put("keyTab", keytab);
                options.put("principal", principal);
                options.put("useKeyTab", "true");
                options.put("storeKey", "true");
                options.put("doNotPrompt", "true");
                options.put("useTicketCache", "true");
                options.put("renewTGT", "true");
                options.put("isInitiator", "false");
            }
            options.put("refreshKrb5Config", "true");
            String ticketCache = System.getenv("KRB5CCNAME");
            if (ticketCache != null) {
                if (IBM_JAVA) {
                    options.put("useDefaultCcache", "true");
                    // The first value searched when "useDefaultCcache" is used.
                    System.setProperty("KRB5CCNAME", ticketCache);
                    options.put("renewTGT", "true");
                    options.put("credsType", "both");
                } else {
                    options.put("ticketCache", ticketCache);
                }
            }
            if (LOG.isDebugEnabled()) {
                options.put("debug", "true");
            }

            return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            options),};
        }
    }

    /**
     * Constant that identifies the authentication mechanism.
     */
    public static final String TYPE = "kerberos";

    /**
     * Constant for the configuration property that indicates the kerberos principal.
     */
    public static final String PRINCIPAL = TYPE + ".principal";

    /**
     * Constant for the configuration property that indicates the keytab file path.
     */
    public static final String KEYTAB = TYPE + ".keytab";

    /**
     * Constant for the configuration property that indicates the Trusted Proxy setting.
     */
    public static final String ENABLE_TRUSTED_PROXY = "enable.trusted.proxy";


    /**
     * Constant for the configuration property that indicates the Kerberos name
     * rules for the Kerberos principals.
     */
    public static final String NAME_RULES = TYPE + ".name.rules";

    public static final String QUERY_STRING_DELIMITER = "&";
    public static final String DOAS_QUERY_STRING = "doAs=";

    private String type;
    private String keytab;
    private GSSManager gssManager;
    private Subject serverSubject = new Subject();
    private List<LoginContext> loginContexts = new ArrayList<LoginContext>();
    private String[] nonBrowserUserAgents;
    private boolean trustedProxyEnabled = false;
    private ProxyUserAuthorization proxyUserAuthorization;

    /**
     * Creates a Kerberos SPNEGO authentication handler with the default
     * auth-token type, <code>kerberos</code>.
     */
    public KerberosAuthenticationHandler() {
        this(TYPE);
    }

    /**
     * Creates a Kerberos SPNEGO authentication handler with a custom auth-token
     * type.
     *
     * @param type auth-token type.
     */
    public KerberosAuthenticationHandler(String type) {
        this.type = type;
    }

    /**
     * Initializes the authentication handler instance.
     * <p>
     * It creates a Kerberos context using the principal and keytab specified in the configuration.
     * <p>
     * This method is invoked by the {@link AuthenticationFilter#init} method.
     *
     * @param config configuration properties to initialize the handler.
     *
     * @throws ServletException thrown if the handler could not be initialized.
     */
    @Override
    public void init(Properties config) throws ServletException {
        try {
            nonBrowserUserAgents = Utils.getNonBrowserUserAgents(config.getProperty(
                    NON_BROWSER_USER_AGENTS, NON_BROWSER_USER_AGENTS_DEFAULT));
            String principal = config.getProperty(PRINCIPAL);
            if (principal == null || principal.trim().length() == 0) {
                throw new ServletException("Principal not defined in configuration");
            }
            keytab = config.getProperty(KEYTAB, keytab);
            if (keytab == null || keytab.trim().length() == 0) {
                throw new ServletException("Keytab not defined in configuration");
            }
            if (!new File(keytab).exists()) {
                throw new ServletException("Keytab does not exist: " + keytab);
            }

            // use all SPNEGO principals in the keytab if a principal isn't
            // specifically configured
            final String[] spnegoPrincipals;
            if (principal.equals("*")) {
                spnegoPrincipals = KerberosUtil.getPrincipalNames(
                        keytab, Pattern.compile("HTTP/.*"));
                if (spnegoPrincipals.length == 0) {
                    throw new ServletException("Principals do not exist in the keytab");
                }
            } else {
                spnegoPrincipals = new String[]{principal};
            }

            String nameRules = config.getProperty(NAME_RULES, null);
            if (nameRules != null) {
                KerberosName.setRules(nameRules);
            }

            for (String spnegoPrincipal : spnegoPrincipals) {
                LOG.info("Login using keytab {}, for principal {}",
                        keytab, spnegoPrincipal);
                final KerberosConfiguration kerberosConfiguration =
                        new KerberosConfiguration(keytab, spnegoPrincipal);
                final LoginContext loginContext =
                        new LoginContext("", serverSubject, null, kerberosConfiguration);
                try {
                    loginContext.login();
                } catch (LoginException le) {
                    LOG.warn("Failed to login as [{}]", spnegoPrincipal, le);
                    throw new AuthenticationException(le);
                }
                loginContexts.add(loginContext);
            }

            trustedProxyEnabled = Boolean.parseBoolean(config.getProperty(ENABLE_TRUSTED_PROXY));
            if (trustedProxyEnabled) {
                proxyUserAuthorization = new ProxyUserAuthorization(config);
            }

            try {
                gssManager = Subject.doAs(serverSubject, new PrivilegedExceptionAction<GSSManager>() {

                    @Override
                    public GSSManager run() throws Exception {
                        return GSSManager.getInstance();
                    }
                });
            } catch (PrivilegedActionException ex) {
                throw ex.getException();
            }
        } catch (Exception ex) {
            throw new ServletException(ex);
        }
    }

    /**
     * Releases any resources initialized by the authentication handler.
     * <p>
     * It destroys the Kerberos context.
     */
    @Override
    public void destroy() {
        keytab = null;
        serverSubject = null;
        for (LoginContext loginContext : loginContexts) {
            try {
                loginContext.logout();
            } catch (LoginException ex) {
                LOG.warn(ex.getMessage(), ex);
            }
        }
        loginContexts.clear();
    }

    /**
     * Returns the authentication type of the authentication handler, 'kerberos'.
     * <p>
     *
     * @return the authentication type of the authentication handler, 'kerberos'.
     */
    @Override
    public String getType() {
        return type;
    }

    /**
     * Returns the Kerberos principals used by the authentication handler.
     *
     * @return the Kerberos principals used by the authentication handler.
     */
    protected Set<KerberosPrincipal> getPrincipals() {
        return serverSubject.getPrincipals(KerberosPrincipal.class);
    }

    /**
     * Returns the keytab used by the authentication handler.
     *
     * @return the keytab used by the authentication handler.
     */
    protected String getKeytab() {
        return keytab;
    }

    /**
     * This is an empty implementation, it always returns <code>TRUE</code>.
     *
     *
     *
     * @param token the authentication token if any, otherwise <code>NULL</code>.
     * @param request the HTTP client request.
     * @param response the HTTP client response.
     *
     * @return <code>TRUE</code>
     * @throws IOException it is never thrown.
     * @throws AuthenticationException it is never thrown.
     */
    @Override
    public boolean managementOperation(AuthenticationToken token,
                                       HttpServletRequest request,
                                       HttpServletResponse response)
            throws IOException, AuthenticationException {
        return true;
    }

    /**
     * It enforces the the Kerberos SPNEGO authentication sequence returning an {@link AuthenticationToken} only
     * after the Kerberos SPNEGO sequence has completed successfully.
     *
     * @param request the HTTP client request.
     * @param response the HTTP client response.
     *
     * @return an authentication token if the Kerberos SPNEGO sequence is complete and valid,
     *         <code>null</code> if it is in progress (in this case the handler handles the response to the client).
     *
     * @throws IOException thrown if an IO error occurred.
     * @throws AuthenticationException thrown if Kerberos SPNEGO sequence failed.
     */
    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, final HttpServletResponse response)
            throws IOException, AuthenticationException {
        AuthenticationToken token = null;
        String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);

        if (authorization == null || !authorization.startsWith(KerberosAuthenticator.NEGOTIATE)) {
            response.setHeader(WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            if (authorization == null) {
                LOG.trace("SPNEGO starting");
            } else {
                LOG.warn("'" + KerberosAuthenticator.AUTHORIZATION + "' does not start with '" +
                        KerberosAuthenticator.NEGOTIATE + "' :  {}", authorization);
            }
        } else {
            authorization = authorization.substring(KerberosAuthenticator.NEGOTIATE.length()).trim();
            final byte[] clientToken = Base64.getDecoder().decode(authorization);
            final String serverName = request.getServerName();
            try {
                token = Subject.doAs(serverSubject, new PrivilegedExceptionAction<AuthenticationToken>() {

                    @Override
                    public AuthenticationToken run() throws Exception {
                        AuthenticationToken token = null;
                        GSSContext gssContext = null;
                        GSSCredential gssCreds = null;
                        try {
                            gssCreds = gssManager.createCredential(
                                    gssManager.createName(
                                            KerberosUtil.getServicePrincipal("HTTP", serverName),
                                            KerberosUtil.getOidInstance("NT_GSS_KRB5_PRINCIPAL")),
                                    GSSCredential.INDEFINITE_LIFETIME,
                                    new Oid[]{
                                            KerberosUtil.getOidInstance("GSS_SPNEGO_MECH_OID"),
                                            KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID")},
                                    GSSCredential.ACCEPT_ONLY);
                            gssContext = gssManager.createContext(gssCreds);
                            byte[] serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.length);
                            if (serverToken != null && serverToken.length > 0) {
                                String authenticate = Base64.getEncoder().encodeToString(serverToken);
                                response.setHeader(KerberosAuthenticator.WWW_AUTHENTICATE,
                                        KerberosAuthenticator.NEGOTIATE + " " + authenticate);
                            }
                            if (!gssContext.isEstablished()) {
                                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                                LOG.trace("SPNEGO in progress");
                            } else {
                                String clientPrincipal = gssContext.getSrcName().toString();
                                KerberosName kerberosName = new KerberosName(clientPrincipal);
                                String userName = kerberosName.getShortName();
                                String doAsUser = null;
                                if (trustedProxyEnabled && (doAsUser = getDoasUser(request)) != null) {
                                    if (!proxyUserAuthorization.authorize(userName, request.getRemoteAddr())) {
                                        LOG.info("{} is not authorized to act as proxy user from {}", userName, request.getRemoteAddr());
                                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                                        return null;
                                    }
                                    clientPrincipal = userName = doAsUser;
                                }
                                token = new AuthenticationToken(userName, clientPrincipal, getType());
                                response.setStatus(HttpServletResponse.SC_OK);
                                LOG.trace("SPNEGO completed for principal [{}]", clientPrincipal);
                            }
                        } finally {
                            if (gssContext != null) {
                                gssContext.dispose();
                            }
                            if (gssCreds != null) {
                                gssCreds.dispose();
                            }
                        }
                        return token;
                    }
                });
            } catch (PrivilegedActionException ex) {
                if (ex.getException() instanceof IOException) {
                    throw (IOException) ex.getException();
                } else {
                    throw new AuthenticationException(ex.getException());
                }
            }
        }
        return token;
    }

    @Override
    public boolean shouldAuthenticate(HttpServletRequest request) {
        boolean isSSOEnabled = false;
        if ((request.getAttribute(JWT_SSO_ENABLED) != null) && Boolean.TRUE.toString().equals(request.getAttribute(JWT_SSO_ENABLED))) {
            isSSOEnabled = true;
        }
        LOG.debug("isSSOEnabled is " + isSSOEnabled);
        if (Utils.isBrowser(nonBrowserUserAgents, request) && isSSOEnabled) {
            return false;
        }
        return true;
    }

    protected static String getDoasUser(HttpServletRequest request) {
        String doAsUser = "";
        String queryString = request.getQueryString();
        if (queryString != null) {
            String[] pairs = queryString.split(QUERY_STRING_DELIMITER);
            try {
                for (String pair : pairs) {
                    if (pair.startsWith(DOAS_QUERY_STRING)) {
                        doAsUser = URLDecoder.decode(pair.substring(DOAS_QUERY_STRING.length()), "UTF-8").trim();
                    }
                }
            } catch (UnsupportedEncodingException ex) {
                //We are providing "UTF-8". This should not be happening ideally.
                LOG.error("Invalid encoding provided.");
            }
        }
        return doAsUser.isEmpty() ? null : doAsUser;
    }

    /**
     * Utility class to support proxy user authorizations.
     *
     * An example config will look like this:
     * proxyuser.knox.hosts = 10.222.0.0
     * proxyuser.admin.hosts = 10.222.0.0,10.113.221.221
     */
    class ProxyUserAuthorization {

        /**
         * Regex for the configuration property that indicates the Trusted Proxy user against approved host list.
         */
        static final String PROXYUSER_REGEX_PATTERN = "proxyuser\\.(.+)\\.hosts";
        static final String DELIMITER = "\\s*,\\s*";

        private Map<String, List<String>> proxyUserHostsMap = new HashMap<>();

        ProxyUserAuthorization(Properties config) {
            Pattern pattern = Pattern.compile(PROXYUSER_REGEX_PATTERN);
            config.stringPropertyNames().forEach((propertyName -> {
                Matcher matcher = pattern.matcher(propertyName);
                if (matcher.find()) {
                    String proxyUser = matcher.group(1);
                    String hostList = config.getProperty(propertyName).trim();
                    List<String> hosts = hostList.equals("*") ? new ArrayList<>() : Arrays.asList(hostList.split(DELIMITER));
                    proxyUserHostsMap.put(proxyUser, hosts);
                }
            }));
        }

        public boolean authorize(String proxyUser, String host) {
            List<String> proxyHosts = proxyUserHostsMap.get(proxyUser);
            if (proxyHosts == null) {
                return false;
            } else if (proxyHosts.isEmpty()) {
                return true;
            }

            return proxyHosts.contains(host);
        }

    }
}
