package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.ranger.authorization.schemaregistry.authorizer.Authorizer;
import org.apache.ranger.authorization.schemaregistry.authorizer.RangerSchemaRegistryAuthorizer;

public enum RangerSchemaRegistryAuthorizationAgent {
    INSTANCE;

    private Authorizer authorizer = new RangerSchemaRegistryAuthorizer();
    private boolean isAuthOn = false;

    public Collection<SchemaVersionInfo> getAllSchemaVersionsWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
    throws SchemaNotFoundException, AuthorizationException {

        if (isAuthOn) {
            SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();

            boolean hasAccessToBranch = authorizer.authorizeSchemaBranch(sGroup,
                    sName,
                    schemaBranchName,
                    Authorizer.ACCESS_TYPE_READ,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
            raiseAuthorizationExceptionIfNeeded(hasAccessToBranch);

            Collection<SchemaVersionInfo> versions = func.get();
            //Only accessible versions will be kept
            return versions.stream().filter(sVersion ->
                    authorizer.authorizeSchemaVersion(sGroup,
                            sName,
                            schemaBranchName,
                            sVersion.getVersion().toString(),
                            Authorizer.ACCESS_TYPE_READ,
                            getUserNameFromSC(sc),
                            getUserGroupsFromSC(sc)))
                    .collect(Collectors.toSet());
        }

        return func.get();
    }

    public SchemaVersionInfo getSchemaVersionWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             Integer schemaVersion,
             SupplierWithSchemaNotFoundException<SchemaVersionInfo> func)
            throws SchemaNotFoundException, AuthorizationException {
        if (isAuthOn) {
            SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();

            boolean hasAccessToVersion = authorizer.authorizeSchemaVersion(sGroup,
                    sName,
                    schemaBranchName,
                    schemaVersion.toString(),
                    Authorizer.ACCESS_TYPE_READ,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
            raiseAuthorizationExceptionIfNeeded(hasAccessToVersion);
        }

        return func.get();
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

    @FunctionalInterface
    public interface SupplierWithSchemaNotFoundException<T> {
        T get() throws SchemaNotFoundException;
    }

}
