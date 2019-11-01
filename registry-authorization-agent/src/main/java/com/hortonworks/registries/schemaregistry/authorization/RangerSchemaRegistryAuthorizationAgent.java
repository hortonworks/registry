package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.security.authorize.AuthorizationException;
import com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer.Authorizer;
import com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer.RangerSchemaRegistryAuthorizer;

public enum RangerSchemaRegistryAuthorizationAgent {
    INSTANCE;

    private Authorizer authorizer = new RangerSchemaRegistryAuthorizer();
    private boolean isAuthOn = true; // TODO: Read it from SR congfig

    public SchemaMetadataInfo getSchemaInfoWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        boolean hasAccessToSchemaMetadata = authorizer.authorizeSchema(sGroup,
                sName,
                Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccessToSchemaMetadata);

        return schemaMetadataInfo;
    }

    public void deleteSchemaMetadataWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws  AuthorizationException {

        if (isAuthOn) {
            SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();

            boolean hasAccess = authorizer.authorizeSchema(sGroup,
                    sName,
                    Authorizer.ACCESS_TYPE_DELETE,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
            raiseAuthorizationExceptionIfNeeded(hasAccess);
        }
    }

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

            boolean hasAccessToVersions = authorizer.authorizeSchemaVersion(sGroup,
                    sName,
                    schemaBranchName,
                    Authorizer.ACCESS_TYPE_READ,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
            raiseAuthorizationExceptionIfNeeded(hasAccessToVersions);
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
                    Authorizer.ACCESS_TYPE_READ,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
            raiseAuthorizationExceptionIfNeeded(hasAccessToVersion);
        }

        return func.get();
    }

    private String getUserNameFromSC(SecurityContext sc) {
        //TODO: Add correct implementation
        if(sc.getUserPrincipal() != null) {
            return sc.getUserPrincipal().getName();
        }

        return "vagrant";
    }

    private Set<String> getUserGroupsFromSC(SecurityContext sc) {
        Set<String> res = new HashSet<>();
        //TODO: Add correct implementation
        res.add("vagrant");

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
