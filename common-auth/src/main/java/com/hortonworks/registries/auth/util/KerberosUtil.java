/**
 * Copyright 2016-2021 Cloudera, Inc.
 *
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
package com.hortonworks.registries.auth.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import javax.security.auth.kerberos.KerberosPrincipal;

public class KerberosUtil {

    public static final Oid GSS_SPNEGO_MECH_OID =
        getNumericOidInstance("1.3.6.1.5.5.2");
    public static final Oid GSS_KRB5_MECH_OID =
        getNumericOidInstance("1.2.840.113554.1.2.2");
    public static final Oid NT_GSS_KRB5_PRINCIPAL_OID =
        getNumericOidInstance("1.2.840.113554.1.2.2.1");

    // numeric oids will never generate a GSSException for a malformed oid.
    // use to initialize statics.
    private static Oid getNumericOidInstance(String oidName) {
        try {
            return new Oid(oidName);
        } catch (GSSException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /* Return the Kerberos login module name */
    public static String getKrb5LoginModuleName() {
        return System.getProperty("java.vendor").contains("IBM")
                ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
    }

    /**
     * Returns the Oid instance from string oidName.
     * Use {@link GSS_SPNEGO_MECH_OID}, {@link GSS_KRB5_MECH_OID},
     * or {@link NT_GSS_KRB5_PRINCIPAL_OID} instead.
     *
     * @return Oid instance
     * @param oidName The oid Name
     * @throws NoSuchFieldException if the input is not supported.
     */
    @Deprecated
    public static Oid getOidInstance(String oidName)
        throws NoSuchFieldException {
        switch (oidName) {
            case "GSS_SPNEGO_MECH_OID":
                return GSS_SPNEGO_MECH_OID;
            case "GSS_KRB5_MECH_OID":
                return GSS_KRB5_MECH_OID;
            case "NT_GSS_KRB5_PRINCIPAL":
                return NT_GSS_KRB5_PRINCIPAL_OID;
            default:
                throw new NoSuchFieldException(
                    "oidName: " + oidName + " is not supported.");
        }
    }

    /**
     * Return the default realm for this JVM.
     *
     * @return The default realm
     * @throws IllegalArgumentException If the default realm does not exist.
     * @throws ClassNotFoundException Not thrown. Exists for compatibility.
     * @throws NoSuchMethodException Not thrown. Exists for compatibility.
     * @throws IllegalAccessException Not thrown. Exists for compatibility.
     * @throws InvocationTargetException Not thrown. Exists for compatibility.
     */
    public static String getDefaultRealm()
        throws ClassNotFoundException, NoSuchMethodException,
        IllegalArgumentException, IllegalAccessException,
        InvocationTargetException {
        // Any name is okay.
        return new KerberosPrincipal("tmp", 1).getRealm();
    }

    /* Return fqdn of the current host */
    static String getLocalHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    /**
     * Create Kerberos principal for a given service and hostname. It converts
     * hostname to lower case. If hostname is null or "0.0.0.0", it uses
     * dynamically looked-up fqdn of the current host instead.
     *
     * @param service
     *          Service for which you want to generate the principal.
     * @param hostname
     *          Fully-qualified domain name.
     * @return Converted Kerberos principal name.
     * @throws UnknownHostException
     *           If no IP address for the local host could be found.
     */
    public static final String getServicePrincipal(String service, String hostname)
            throws UnknownHostException {
        String fqdn = hostname;
        if (null == fqdn || fqdn.equals("") || fqdn.equals("0.0.0.0")) {
            fqdn = getLocalHostName();
        }
        // convert hostname to lowercase as kerberos does not work with hostnames
        // with uppercase characters.
        return service + "/" + fqdn.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Get all the unique principals present in the keytabfile.
     *
     * @param keytabFileName
     *          Name of the keytab file to be read.
     * @return list of unique principals in the keytab.
     * @throws IOException
     *          If keytab entries cannot be read from the file.
     */
    static final String[] getPrincipalNames(String keytabFileName) throws IOException {
        Keytab keytab = Keytab.read(new File(keytabFileName));
        Set<String> principals = new HashSet<String>();
        List<KeytabEntry> entries = keytab.getEntries();
        for (KeytabEntry entry : entries) {
            principals.add(entry.getPrincipalName().replace("\\", "/"));
        }
        return principals.toArray(new String[0]);
    }

    /**
     * Get all the unique principals from keytabfile which matches a pattern.
     *
     * @param keytab Name of the keytab file to be read.
     * @param pattern pattern to be matched.
     * @return list of unique principals which matches the pattern.
     * @throws IOException if cannot get the principal name
     */
    public static final String[] getPrincipalNames(String keytab,
                                                   Pattern pattern) throws IOException {
        String[] principals = getPrincipalNames(keytab);
        if (principals.length != 0) {
            List<String> matchingPrincipals = new ArrayList<String>();
            for (String principal : principals) {
                if (pattern.matcher(principal).matches()) {
                    matchingPrincipals.add(principal);
                }
            }
            principals = matchingPrincipals.toArray(new String[0]);
        }
        return principals;
    }

    private KerberosUtil() { }
}
