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
import java.util.stream.Stream;

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
        String sGroup = aggregatedSchemaMetadataInfo.getSchemaMetadata().getSchemaGroup();
        String sName = aggregatedSchemaMetadataInfo.getSchemaMetadata().getName();

        boolean accessToSchemaMetadata = authorizer.authorizeSchema(sGroup,
                sName,
                Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(accessToSchemaMetadata);

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
    public void addSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.ACCESS_TYPE_CREATE);
    }

    @Override
    public void getLatestSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.ACCESS_TYPE_READ);
    }

    @Override
    public void authorizeGetSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.ACCESS_TYPE_READ);
    }


    @Override
    public void authorizeVerisonStateOperation(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.ACCESS_TYPE_UPDATE);
    }

    @Override
    public void checkCompatibilityWithSchema(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.ACCESS_TYPE_READ);
    }

    @Override
    public void getSerializers(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {
        boolean hasAccessToSerdes = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));

        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        boolean hasAccesToSchema = authorizer.authorizeSchema(sGroup,
                sName,
                Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes && hasAccesToSchema);
    }

    @Override
    public void uploadFile(SecurityContext sc) throws AuthorizationException {
        boolean hasAccessToSerdes = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_UPDATE,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes);
    }

    @Override
    public void downloadFile(SecurityContext sc) throws AuthorizationException {
        boolean hasAccessToSerdes = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes);
    }

    @Override
    public void addSerDes(SecurityContext sc) throws AuthorizationException {
        boolean hasAccessToSerdes = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_CREATE,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes);
    }

    @Override
    public void getSerDes(SecurityContext sc) throws AuthorizationException {
        boolean hasAccessToSerdes = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes);
    }

    @Override
    public void mapSchemaWithSerDes(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {
        boolean hasAccessToSerdes = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes);
    }

    @Override
    public void deleteSchemaVersion(SecurityContext sc,
                                    SchemaMetadataInfo schemaMetadataInfo,
                                    Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.ACCESS_TYPE_READ);
    }

    @Override
    public Collection<SchemaBranch> getAllBranches(SecurityContext sc,
                                                   SchemaMetadataInfo schemaMetadataInfo,
                                                   SupplierWithSchemaNotFoundException<Collection<SchemaBranch>> func)
            throws SchemaNotFoundException {
        return func.get().stream().filter(branch -> {
            SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();

            return authorizer.authorizeSchemaBranch(sGroup,
                    sName,
                    branch.getName(),
                    Authorizer.ACCESS_TYPE_READ,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
        }).collect(Collectors.toList());
    }

    @Override
    public void createSchemaBranch(SecurityContext sc,
                                   SchemaMetadataInfo schemaMetadataInfo,
                                   Collection<SchemaBranch> branches,
                                   String branchTocreate) throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        boolean permsToCreateBranches = authorizer.authorizeSchemaBranch(sGroup,
                sName,
                branchTocreate,
                Authorizer.ACCESS_TYPE_CREATE,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));

        boolean permsToReadVersions= authorizer.authorizeSchemaVersion(sGroup,
                sName,
                getPrimaryBranch(branches).getName(),
                Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToCreateBranches && permsToReadVersions);
    }

    @Override
    public void mergeSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        boolean permsToReadVersions = authorizer.authorizeSchemaVersion(sGroup,
                sName,
                getPrimaryBranch(branches).getName(),
                Authorizer.ACCESS_TYPE_READ,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));

        boolean permsToCreateVersions = authorizer.authorizeSchemaVersion(sGroup,
                sName,
                SchemaBranch.MASTER_BRANCH,
                Authorizer.ACCESS_TYPE_CREATE,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToCreateVersions && permsToReadVersions);
    }

    @Override
    public void deleteSchemaBranch(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        boolean permsToDelete = authorizer.authorizeSchemaBranch(sGroup,
                sName,
                schemaBranch,
                Authorizer.ACCESS_TYPE_DELETE,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(permsToDelete);
    }

    ///////////////////// ConfluenceCompatible APIs /////////////////////

    @Override
    public Stream<SchemaMetadataInfo> getSubjects(SecurityContext sc, Stream<SchemaMetadataInfo> stream) {
        return stream.filter(sM ->
                authorizer.authorizeSchema(sM.getSchemaMetadata().getSchemaGroup(),
                        sM.getSchemaMetadata().getName(),
                        Authorizer.ACCESS_TYPE_READ,
                        getUserNameFromSC(sc),
                        getUserGroupsFromSC(sc)));
    }

    @Override
    public Stream<SchemaVersionInfo> getAllVersions(SecurityContext sc,
                                                    Stream<SchemaVersionInfo> vStream,
                                                    FunctionWithSchemaNotFoundException<Long, SchemaMetadataInfo> getMetadataFunc,
                                                    FunctionWithBranchSchemaNotFoundException<Long, Collection<SchemaBranch>> getBranches) {
        return vStream.filter(schemaVersionInfo -> {
            SchemaMetadata sM;
            try {
                sM = getMetadataFunc.apply(schemaVersionInfo.getSchemaMetadataId()).getSchemaMetadata();
            } catch (SchemaNotFoundException e) {
               throw new RuntimeException(e);
            }
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();
            String sBranch = getPrimaryBranch(getBranches.apply(schemaVersionInfo.getId())).getName();

            return authorizer.authorizeSchemaVersion(sGroup,
                    sName,
                    sBranch,
                    Authorizer.ACCESS_TYPE_READ,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
        });
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

    private void authorizeSchemaVersionOpInternal(SecurityContext sc,
                                                  SchemaMetadataInfo schemaMetadataInfo,
                                                  String schemaBranch,
                                                  String accessType) throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        boolean hasAccess = authorizer.authorizeSchemaVersion(sGroup,
                sName,
                schemaBranch,
                accessType,
                getUserNameFromSC(sc),
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccess);
    }

    private SchemaBranch getPrimaryBranch(Collection<SchemaBranch> branches) {
        return branches.stream().min((b1, b2) -> b1.getId() < b2.getId() ? -1 :
                b1.getId() > b2.getId() ? 1 : 0).get();
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
