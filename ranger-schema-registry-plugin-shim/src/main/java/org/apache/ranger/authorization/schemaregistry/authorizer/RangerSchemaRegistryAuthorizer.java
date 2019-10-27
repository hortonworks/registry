package org.apache.ranger.authorization.schemaregistry.authorizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import java.util.Set;

public class RangerSchemaRegistryAuthorizer implements Authorizer {

    private static final Log LOG = LogFactory.getLog(RangerSchemaRegistryAuthorizer.class);

    private static final String   RANGER_PLUGIN_TYPE                      = "schema-registry";
    private static final String   RANGER_SR_AUTHORIZER_IMPL_CLASSNAME  =
            "org.apache.ranger.authorization.schemaregistry.authorizer.RangerSchemaRegistryAuthorizerImpl";

    private Authorizer  rangerSRAuthorizerImpl;
    private static RangerPluginClassLoader rangerPluginClassLoader;

    public RangerSchemaRegistryAuthorizer() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerSchemaRegistryAuthorizer.RangerKafkaAuthorizer()");
        }

        this.init();

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.RangerKafkaAuthorizer()");
        }
    }

    private void init(){
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerSchemaRegistryAuthorizer.init()");
        }

        try {

            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<Authorizer> cls = (Class<Authorizer>) Class.forName(RANGER_SR_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            activatePluginClassLoader();

            rangerSRAuthorizerImpl = cls.newInstance();
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerSchemaRegistryAuthorizer", e);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.init()");
        }
    }

    @Override
    public boolean authorizeSerDe(String sName,
                                  String accessType,
                                  String uName,
                                  Set<String> uGroup) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                    "==> RangerSchemaRegistryAuthorizer.authorizeSerDe(sName=%s, accessType=%s, uName=%s, uGroup=%s)",
                    sName,
                    accessType,
                    uName,
                    uGroup));
        }

        boolean ret = false;

        try {
            activatePluginClassLoader();
            ret = rangerSRAuthorizerImpl.authorizeSerDe(sName, accessType, uName, uGroup);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.authorizeSerDe: " + ret);
        }

        return ret;
    }

    @Override
    public boolean authorizeSchemaGroup(String sGroupName,
                                        String accessType,
                                        String uName,
                                        Set<String> uGroup) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                    "==> RangerSchemaRegistryAuthorizer.authorizeSchemaGroup(sName=%s, accessType=%s, uName=%s, uGroup=%s)",
                    sGroupName,
                    accessType,
                    uName,
                    uGroup));
        }

        boolean ret = false;

        try {
            activatePluginClassLoader();
            ret = rangerSRAuthorizerImpl.authorizeSchemaGroup(sGroupName, accessType, uName, uGroup);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.authorizeSchemaGroup: " + ret);
        }

        return ret;
    }

    @Override
    public boolean authorizeSchema(String sGroupName,
                                   String sMetadataName,
                                   String accessType,
                                   String uName,
                                   Set<String> uGroup) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                    "==> RangerSchemaRegistryAuthorizer.authorizeSchema"
                            + "(sName=%s, sMetadataName=%s, accessType=%s, uName=%s, uGroup=%s)",
                    sGroupName,
                    sMetadataName,
                    accessType,
                    uName,
                    uGroup));
        }

        boolean ret = false;

        try {
            activatePluginClassLoader();
            ret = rangerSRAuthorizerImpl.authorizeSchema(sGroupName, sMetadataName, accessType, uName, uGroup);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.authorizeSchema: " + ret);
        }

        return ret;
    }

    @Override
    public boolean authorizeSchemaBranch(String sGroupName,
                                         String sMetadataName,
                                         String sBranchName,
                                         String accessType,
                                         String uName,
                                         Set<String> uGroup) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                    "==> RangerSchemaRegistryAuthorizer.authorizeSchemaBranch"
                            + "(sName=%s, sMetadataName=%s, sBranchName=%s, accessType=%s, uName=%s, uGroup=%s)",
                    sGroupName,
                    sMetadataName,
                    sBranchName,
                    accessType,
                    uName,
                    uGroup));
        }

        boolean ret = false;

        try {
            activatePluginClassLoader();
            ret = rangerSRAuthorizerImpl.authorizeSchemaBranch(sGroupName, sMetadataName, sBranchName, accessType, uName, uGroup);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.authorizeSchemaBranch: " + ret);
        }

        return ret;
    }

    @Override
    public boolean authorizeSchemaVersion(String sGroupName,
                                          String sMetadataName,
                                          String sBranchName,
                                          String sVersion,
                                          String accessType,
                                          String uName,
                                          Set<String> uGroup) {

        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                    "==> RangerSchemaRegistryAuthorizer.authorizeSchemaVersion"
                       + "(sName=%s, sMetadataName=%s, sBranchName=%s, sVersion=%s, accessType=%s, uName=%s, uGroup=%s)",
                    sGroupName,
                    sMetadataName,
                    sBranchName,
                    sVersion,
                    accessType,
                    uName,
                    uGroup));
        }

        boolean ret = false;

        try {
            activatePluginClassLoader();
            ret = rangerSRAuthorizerImpl.authorizeSchemaVersion(sGroupName,
                    sMetadataName,
                    sBranchName,
                    sVersion,
                    accessType,
                    uName,
                    uGroup);
        } finally {
            deactivatePluginClassLoader();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSchemaRegistryAuthorizer.authorizeSchemaVersion: " + ret);
        }

        return ret;
    }

    private void activatePluginClassLoader() {
        if(rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if(rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }
}
