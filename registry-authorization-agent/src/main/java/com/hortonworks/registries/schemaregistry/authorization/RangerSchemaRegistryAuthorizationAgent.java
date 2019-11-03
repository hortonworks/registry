package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hadoop.security.authorize.AuthorizationException;
import com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer.Authorizer;
import com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer.RangerSchemaRegistryAuthorizer;

public enum RangerSchemaRegistryAuthorizationAgent implements AuthorizationAgent {
    INSTANCE;

    private Authorizer authorizer = new RangerSchemaRegistryAuthorizer();

    @Override
    public Collection<AggregatedSchemaMetadataInfo> listAggregatedSchemas(SecurityContext sc, SupplierWithSchemaNotFoundException<Collection<AggregatedSchemaMetadataInfo>> func) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public AggregatedSchemaMetadataInfo getAggregatedSchemaInfo(SecurityContext sc, AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo) throws AuthorizationException {
        return null;
    }

    @Override
    public Collection<SchemaMetadataInfo> findSchemas(SecurityContext sc,
                                                      Supplier<Collection<SchemaMetadataInfo>> func) {
        return func.get().stream()
                .filter(schemaMetadataInfo -> {
                    SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
                    String sGroup = schemaMetadata.getSchemaGroup();
                    String sName = schemaMetadata.getName();
                    return authorizer.authorizeSchema(sGroup,
                            sName,
                            Authorizer.ACCESS_TYPE_READ,
                            getUserNameFromSC(sc),
                            getUserGroupsFromSC(sc));
                }).collect(Collectors.toList());
    }

    @Override
    public List<AggregatedSchemaMetadataInfo> findAggregatedSchemas(SecurityContext sc, List<AggregatedSchemaMetadataInfo> asmi) {
        return null;
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFieldsWithAuthorization
            (SecurityContext sc,
             Function<String, SchemaMetadataInfo> getSchemaMetadataFunc,
             FunctionWithSchemaNotFoundException <SchemaVersionKey, SchemaVersionInfo> getVersionInfoFunc,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionKey>> func)
            throws SchemaNotFoundException {

        return func.get().stream()
                .filter(schemaVersionKey -> {
                    String sName = schemaVersionKey.getSchemaName();
                    String sGroup = getSchemaMetadataFunc.apply(sName).getSchemaMetadata().getSchemaGroup();
                    SchemaVersionInfo svi;
                    try {
                        svi = getVersionInfoFunc.apply(schemaVersionKey);
                    } catch (SchemaNotFoundException e) {
                       throw new RuntimeException(e);
                    }
                    String sBranch = svi.getMergeInfo().getSchemaBranchName();
                    return authorizer.authorizeSchemaVersion(sGroup,
                            sName,
                            sBranch,
                            Authorizer.ACCESS_TYPE_READ,
                            getUserNameFromSC(sc),
                            getUserGroupsFromSC(sc));
                }).collect(Collectors.toList());

    }

    @Override
    public void  addSchemaInfoWithAuthorization(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException {
        String sGroup = schemaMetadata.getSchemaGroup();
        String sName = schemaMetadata.getName();

        boolean hasAccess = authorizer.authorizeSchema(sGroup,
                sName,
                Authorizer.ACCESS_TYPE_CREATE,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccess);
    }

    @Override
    public void updateSchemaInfoWithAuthorization(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException {
        String sGroup = schemaMetadata.getSchemaGroup();
        String sName = schemaMetadata.getName();

        boolean hasAccess = authorizer.authorizeSchema(sGroup,
                sName,
                Authorizer.ACCESS_TYPE_UPDATE,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccess);
    }

    @Override
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

    @Override
    public void deleteSchemaMetadataWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws  AuthorizationException {

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

    @Override
    public void uploadSchemaVersion(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void addSchemaVersion(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void getLatestSchemaVersion(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void getSchemaVersionById(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void getSchemaVersionByFingerprint(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void authorizeVerisonStateOperation(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void checkCompatibilityWithSchema(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void getSerializers(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {

    }

    @Override
    public void uploadFile(SecurityContext securityContext) throws AuthorizationException {

    }

    @Override
    public void downloadFile(SecurityContext securityContext) throws AuthorizationException {

    }

    @Override
    public void addSerDes(SecurityContext securityContext) throws AuthorizationException {

    }

    @Override
    public void getSerDes(SecurityContext securityContext) throws AuthorizationException {

    }

    @Override
    public void mapSchemaWithSerDes(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {

    }

    @Override
    public void deleteSchemaVersion(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public Collection<SchemaBranch> getAllBranches(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, SupplierWithSchemaNotFoundException<Collection<SchemaBranch>> func) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public void createSchemaBranch(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch, String branchTocreate) throws AuthorizationException {

    }

    @Override
    public void mergeSchemaVersion(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public void deleteSchemaBranch(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {

    }

    @Override
    public Collection<SchemaVersionInfo> getAllSchemaVersionsWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
    throws SchemaNotFoundException, AuthorizationException {

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

        return func.get();
    }

    @Override
    public void getSchemaVersionWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName)
            throws AuthorizationException {

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

}
