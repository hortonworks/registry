package org.apache.ranger.authorization.schemaregistry.authorizer;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.security.authorize.AuthorizationException;

public enum SRAuthorizationAgent {
    INSTANCE;

    private static final String PLG_TYPE = "schema-registry";
    private static final String PLG_NAME = "schema-registry";

    private final RangerBasePlugin plg;

    SRAuthorizationAgent() {
        plg = new RangerBasePlugin(PLG_TYPE, PLG_NAME);
        plg.setResultProcessor(new RangerSchemaRegistryAuditHandler());
        plg.init();
    }

    public Collection<SchemaVersionInfo> getAllSchemaVersionsWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
    throws SchemaNotFoundException, AuthorizationException {
        SRAuthorizer authorizer = getAuthorizer(sc);
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        boolean hasAccessToBranch = authorizer.authorizeSchemaBranch(sGroup,
                sName,
                schemaBranchName,
                SRAuthorizer.ACCESS_TYPE_READ);
        raiseAuthorizationExceptionIfNeeded(hasAccessToBranch);

        Collection<SchemaVersionInfo> versions = func.get();
        //Only accessible versions will be kept
        return versions.stream().filter(sVersion ->
                authorizer.authorizeSchemaVersion(sGroup,
                        sName,
                        schemaBranchName,
                        sVersion.getVersion().toString(),
                        SRAuthorizer.ACCESS_TYPE_READ))
                .collect(Collectors.toSet());
    }

    public SchemaVersionInfo getSchemaVersionWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             Integer schemaVersion,
             SupplierWithSchemaNotFoundException<SchemaVersionInfo> func)
            throws SchemaNotFoundException, AuthorizationException {
        SRAuthorizer authorizer = getAuthorizer(sc);
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        boolean hasAccessToVersion = authorizer.authorizeSchemaVersion(sGroup,
                sName,
                schemaBranchName,
                schemaVersion.toString(),
                SRAuthorizer.ACCESS_TYPE_READ);
        raiseAuthorizationExceptionIfNeeded(hasAccessToVersion);

        return func.get();

    }

    private SRAuthorizer getAuthorizer(SecurityContext sc) {
        String uName = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        return new SRDefaultAuthorizer(plg, uName, uGroups);
    }

    private String getUserNameFromSC(SecurityContext sc) {
        return sc.getUserPrincipal().getName();
    }

    private Set<String> getUserGroupsFromSC(SecurityContext sc) {
        Set<String> res = new HashSet<>();
        //TODO: Add correct implementation
        res.add(sc.getUserPrincipal().getName());

        return res;
    }

    private void raiseAuthorizationExceptionIfNeeded(boolean isAuthorized) throws AuthorizationException {
        if(!isAuthorized) {
            throw new AuthorizationException("");
        }
    }

    RangerBasePlugin getPlg() {
        return plg;
    }

    @FunctionalInterface
    public interface SupplierWithSchemaNotFoundException<T> {
        T get() throws SchemaNotFoundException;
    }


}
