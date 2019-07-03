package com.hortonworks.registries.auth.server;

import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.auth.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.kerberos.authentication.KerberosAuthenticationProvider;
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosClient;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

/**
 * The {@link KerberosLoginAuthenticationHandler} augments the Kerberos SPNEGO authentication mechanism with the Kerberos Login authentication
 * mechanism. If a user provides user credentials in a HTTPS, POST call, then a Kerberos login is attempted. In the authentication failure scenario,
 * the SPNEGO sequence is invoked.
 *
 * <p>
 * The supported configuration properties that are required in addition to those required by KerberosAuthenticationHandler are:
 * <ul>
 * <li>login.enabled: a boolean string to indicate whether the enabling of Kerberos login.</li>
 * </ul>
 */
public class KerberosLoginAuthenticationHandler extends KerberosAuthenticationHandler {

    public static final String LOGIN_ENABLED_CONFIG = "login.enabled";
    public static final String SPNEGO_ENABLED_CONFIG = "spnego.enabled";
    public static final String TYPE = "kerberos-login";
    public static final String AUTHORIZATION_HEADER = "Authorization";

    private static final Logger LOG = LoggerFactory.getLogger(KerberosAuthenticationHandler.class);
    private static final String METHOD = "POST";

    private KerberosAuthenticationProvider provider;
    private boolean spnegoEnabled;

    KerberosLoginAuthenticationHandler () {
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties config) throws ServletException {
        spnegoEnabled = Boolean.parseBoolean(config.getProperty(SPNEGO_ENABLED_CONFIG, Boolean.TRUE.toString()));
        if (spnegoEnabled) {
            super.init(config);
        }

        try {
            provider = new KerberosAuthenticationProvider();
            SunJaasKerberosClient client = new SunJaasKerberosClient();
            client.setDebug(true);
            provider.setKerberosClient(client);
            provider.setUserDetailsService(new KerberosUserDetailsService());
        } catch (Exception ex) {
            LOG.error("Failed to initialize the Kerberos Login Authentication Handler.", ex);
            throw new ServletException(ex);
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException {
        AuthenticationToken token = kerberosLogin(request, response);

        if (token == null && spnegoEnabled) {
            LOG.debug("Attempting SPNEGO authentication sequence as kerberos login failed.");
            token = super.authenticate(request, response);
        }

        return token;
    }

    private AuthenticationToken kerberosLogin(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (provider == null) {
            LOG.error("The Kerberos authentication provider is not initialized.");
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }
        String authorization =  request.getHeader(AUTHORIZATION_HEADER);
        if (!request.getMethod().equals(METHOD) || !request.isSecure() || authorization.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos Login is not attempted because method: {}, secure: {}, authorization is empty: {}", request.getMethod(),
                        request.isSecure(), authorization.isEmpty());
            }
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }
        String credentials = authorization.split("Basic ")[1].trim();
        byte[] credentialsArray = Base64.getDecoder().decode(credentials);
        String[] userPassword = new String(credentialsArray, StandardCharsets.UTF_8).split(":");
        if (userPassword.length != 2) {
            LOG.error("Login credentials of invalid length is passed to the Authorization header {}.", userPassword.length);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }
        String rawPrincipal = userPassword[0];
        String password = userPassword[1];

        final KerberosName kerberosName = new KerberosName(rawPrincipal);
        String identity = getUserIdentity(kerberosName, rawPrincipal);

        // Perform the authentication
        final UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(identity, password);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created authentication token for principal {} with name {} and is authenticated {}", token.getPrincipal(),
                    token.getName(), token.isAuthenticated());
        }

        Authentication authentication = null;

        try {
            authentication = provider.authenticate(token);
        } catch (BadCredentialsException ex) {
            LOG.debug("Bad credentials provided", ex);
        } catch (final org.springframework.security.core.AuthenticationException ex) {
            LOG.error("Kerberos login failed.", ex);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ran provider.authenticate() and returned authentication for " +
                            "principal {} with name {} and is authenticated {}", authentication.getPrincipal(), authentication.getName(),
                    authentication.isAuthenticated());
        }

        if (authentication == null) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            return new AuthenticationToken(kerberosName.getShortName(), authentication.getName(), getType());
        }
    }

    /**
     * Get the kerberos user principal.
     */
    private String getUserIdentity(KerberosName kerberosName, String rawPrincipal) {
        String defaultRealm = kerberosName.getDefaultRealm();
        // The default realm configured from the krb5 configuration will end up being used
        boolean realmInRawPrincipal = !Utils.isBlank(kerberosName.getRealm());
        final String identity;
        if (realmInRawPrincipal) {
            // there's a realm already in the given principal, use it
            identity = rawPrincipal;
            LOG.debug("Realm was specified in principal {}, default realm was not added to the identity being authenticated", rawPrincipal);
        } else if (!Utils.isBlank(defaultRealm)) {
            // the value for the default realm is not blank, append the realm to the given principal
            identity = rawPrincipal + "@" + defaultRealm;
            LOG.debug("Realm was not specified in principal {}, default realm {} was added to the identity being authenticated", rawPrincipal,
                    defaultRealm);
        } else {
            // otherwise, use the given principal, which will use the default realm as specified in the krb5 configuration
            identity = rawPrincipal;
            LOG.debug("Realm was not specified in principal {}, default realm is blank and was not added to the identity being authenticated",
                    rawPrincipal);
        }

        return identity;
    }

    static class KerberosUserDetailsService implements UserDetailsService {

        @Override
        public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
            return new User(username, "notUsed", true, true, true, true,
                    AuthorityUtils.createAuthorityList("ROLE_USER"));
        }
    }
}
