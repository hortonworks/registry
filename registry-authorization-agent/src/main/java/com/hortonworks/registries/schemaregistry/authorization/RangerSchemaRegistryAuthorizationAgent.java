package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer.Authorizer;
import com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer.RangerSchemaRegistryAuthorizer;

public enum RangerSchemaRegistryAuthorizationAgent implements AuthorizationAgent {
    INSTANCE;

    private Authorizer authorizer = new RangerSchemaRegistryAuthorizer();

    @Override
    public Collection<AggregatedSchemaMetadataInfo> authorizeListAggregatedSchemas
            (SecurityContext sc,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList) {
        return aggregatedSchemaMetadataInfoList
                .stream().map(aggregatedSchemaMetadataInfo -> {
                    try {
                        return authorizeGetAggregatedSchemaInfo(sc, aggregatedSchemaMetadataInfo);
                    } catch (AuthorizationException e) {
                        return null;
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(SecurityContext sc, AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo) throws AuthorizationException {
        String sGroup = aggregatedSchemaMetadataInfo.getSchemaMetadata().getSchemaGroup();
        String sName = aggregatedSchemaMetadataInfo.getSchemaMetadata().getName();

        String user = getUserNameFromSC(sc);
        Set<String> userGroups = getUserGroupsFromSC(sc);

        boolean accessToSchemaMetadata = authorizer.authorizeSchema(sGroup,
                sName,
                Authorizer.ACCESS_TYPE_READ,
                user,
                userGroups);

        raiseAuthorizationExceptionIfNeeded(accessToSchemaMetadata,
                user,
                Authorizer.ACCESS_TYPE_READ,
                Authorizer.RESOURCE_SCHEMA_METADATA,
                compoundNameToString(sGroup, sName));
        Collection<AggregatedSchemaBranch> filteredBranches = aggregatedSchemaMetadataInfo
                .getSchemaBranches()
                .stream().map(branch -> filterAggregatedBranch(sGroup, sName, user, userGroups, branch))
                .filter(Objects::nonNull).collect(Collectors.toList());

        boolean accessToSerdes = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ, user, userGroups);

        Collection<SerDesInfo> serDesInfos;
        if (accessToSerdes) {
            serDesInfos = aggregatedSchemaMetadataInfo.getSerDesInfos();
        } else {
            serDesInfos = new ArrayList<>();
        }

        return new AggregatedSchemaMetadataInfo(aggregatedSchemaMetadataInfo.getSchemaMetadata(),
                aggregatedSchemaMetadataInfo.getId(),
                aggregatedSchemaMetadataInfo.getTimestamp(),
                filteredBranches,
                serDesInfos);
    }

    private AggregatedSchemaBranch filterAggregatedBranch(String sGroup,
                                                          String sName,
                                                          String user,
                                                          Set<String> userGroups,
                                                          AggregatedSchemaBranch branch) {

        boolean accessToBranch = authorizer.authorizeSchemaBranch(sGroup,
                sName,
                branch.getSchemaBranch().getName(),
                Authorizer.ACCESS_TYPE_READ,
                user,
                userGroups);
        if (!accessToBranch) {
            return null;
        }

        Collection<SchemaVersionInfo> filteredVersions = branch
                .getSchemaVersionInfos()
                .stream()
                .filter(version -> authorizer.authorizeSchemaVersion(sGroup,
                        sName,
                        branch.getSchemaBranch().getName(),
                        Authorizer.ACCESS_TYPE_READ,
                        user,
                        userGroups)).collect(Collectors.toList());

        return new AggregatedSchemaBranch(branch.getSchemaBranch(),
                branch.getRootSchemaVersion(),
                filteredVersions);
    }

    @Override
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(SecurityContext sc,
                                                               Collection<SchemaMetadataInfo> schemas) {
        return schemas.stream()
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
    public List<AggregatedSchemaMetadataInfo> authorizeFindAggregatedSchemas(SecurityContext sc, List<AggregatedSchemaMetadataInfo> asmil) {
        return asmil
                .stream().map(aggregatedSchemaMetadataInfo -> {
                    try {
                        return authorizeGetAggregatedSchemaInfo(sc, aggregatedSchemaMetadataInfo);
                    } catch (AuthorizationException e) {
                        return null;
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields
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
    public void authorizeAddSchemaInfo(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException {
        authorizeSchema(sc, schemaMetadata, Authorizer.ACCESS_TYPE_CREATE);
    }

    @Override
    public void authorizeUpdateSchemaInfo(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException {
        authorizeSchema(sc, schemaMetadata, Authorizer.ACCESS_TYPE_UPDATE);
    }

    @Override
    public SchemaMetadataInfo authorizeGetSchemaInfo
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException {

        authorizeSchema(sc, schemaMetadataInfo.getSchemaMetadata(), Authorizer.ACCESS_TYPE_READ);

        return schemaMetadataInfo;
    }

    @Override
    public void authorizeDeleteSchemaMetadata
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws  AuthorizationException {
        authorizeSchema(sc, schemaMetadataInfo.getSchemaMetadata(), Authorizer.ACCESS_TYPE_DELETE);
    }

    private void authorizeSchema(SecurityContext sc,
                                 SchemaMetadata sM,
                                 String accessType)
            throws  AuthorizationException {
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String user = getUserNameFromSC(sc);

        boolean hasAccess = authorizer.authorizeSchema(sGroup,
                sName,
                accessType,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(hasAccess,
                user,
                accessType,
                Authorizer.RESOURCE_SCHEMA_METADATA,
                compoundNameToString(sGroup,sName));
    }

    @Override
    public void authorizeAddSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.ACCESS_TYPE_CREATE);
    }

    @Override
    public void authorizeGetLatestSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.ACCESS_TYPE_READ);
    }

    @Override
    public void authorizeGetSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.ACCESS_TYPE_READ);
    }


    @Override
    public void authorizeVersionStateOperation(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.ACCESS_TYPE_UPDATE);
    }

    @Override
    public void authorizeCheckCompatibilityWithSchema(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.ACCESS_TYPE_READ);
    }

    @Override
    public void authorizeGetSerializers(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.ACCESS_TYPE_READ);
    }

    @Override
    public void authorizeUploadFile(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.ACCESS_TYPE_UPDATE);
    }

    @Override
    public void authorizeDownloadFile(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.ACCESS_TYPE_READ);
    }

    @Override
    public void authorizeAddSerDes(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.ACCESS_TYPE_CREATE);
    }

    @Override
    public void authorizeGetSerDes(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.ACCESS_TYPE_READ);
    }

    private void authorizeSerDes(SecurityContext sc, String accessType) throws AuthorizationException  {
        String user = getUserNameFromSC(sc);
        boolean hasAccessToSerdes = authorizer.authorizeSerDe(accessType,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes,
                user,
                accessType,
                Authorizer.RESOURCE_SERDE,
                "*");
    }

    @Override
    public void authorizeMapSchemaWithSerDes(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.ACCESS_TYPE_READ);
        authorizeSchema(sc, schemaMetadataInfo.getSchemaMetadata(), Authorizer.ACCESS_TYPE_UPDATE);
    }

    @Override
    public void authorizeDeleteSchemaVersion(SecurityContext sc,
                                             SchemaMetadataInfo schemaMetadataInfo,
                                             Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.ACCESS_TYPE_DELETE);
    }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext sc,
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
    public void authorizeCreateSchemaBranch(SecurityContext sc,
                                            SchemaMetadataInfo schemaMetadataInfo,
                                            Collection<SchemaBranch> branches,
                                            String branchTocreate) throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String user = getUserNameFromSC(sc);

        boolean permsToCreateBranches = authorizer.authorizeSchemaBranch(sGroup,
                sName,
                branchTocreate,
                Authorizer.ACCESS_TYPE_CREATE,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToCreateBranches,
                user,
                Authorizer.ACCESS_TYPE_CREATE,
                Authorizer.RESOURCE_SCHEMA_BRANCH,
                compoundNameToString(sGroup, sName, branchTocreate));

        String branch = getPrimaryBranch(branches).getName();
        boolean permsToReadVersions= authorizer.authorizeSchemaVersion(sGroup,
                sName,
                branch,
                Authorizer.ACCESS_TYPE_READ,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToReadVersions,
                user,
                Authorizer.ACCESS_TYPE_READ,
                Authorizer.RESOURCE_SCHEMA_VERSION,
                compoundNameToString(sGroup, sName, branch, "*"));
    }

    @Override
    public void authorizeMergeSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String user = getUserNameFromSC(sc);

        String branch = getPrimaryBranch(branches).getName();
        boolean permsToReadVersions = authorizer.authorizeSchemaVersion(sGroup,
                sName,
                branch,
                Authorizer.ACCESS_TYPE_READ,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToReadVersions,
                user,
                Authorizer.ACCESS_TYPE_READ,
                Authorizer.RESOURCE_SCHEMA_VERSION,
                compoundNameToString(sGroup, sName, branch, "*"));

        boolean permsToCreateVersions = authorizer.authorizeSchemaVersion(sGroup,
                sName,
                SchemaBranch.MASTER_BRANCH,
                Authorizer.ACCESS_TYPE_CREATE,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToCreateVersions,
                user,
                Authorizer.ACCESS_TYPE_CREATE,
                Authorizer.RESOURCE_SCHEMA_VERSION,
                compoundNameToString(sGroup, sName, SchemaBranch.MASTER_BRANCH, "*"));
    }

    @Override
    public void authorizeDeleteSchemaBranch(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        String user = getUserNameFromSC(sc);
        boolean permsToDelete = authorizer.authorizeSchemaBranch(sGroup,
                sName,
                schemaBranch,
                Authorizer.ACCESS_TYPE_DELETE,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToDelete,
                user,
                Authorizer.ACCESS_TYPE_DELETE,
                Authorizer.RESOURCE_SCHEMA_BRANCH,
                compoundNameToString(sGroup, sName, schemaBranch));
    }

    ///////////////////// ConfluenceCompatible APIs /////////////////////

    @Override
    public Stream<SchemaMetadataInfo> authorizeGetSubjects(SecurityContext sc, Stream<SchemaMetadataInfo> stream) {
        return stream.filter(sM ->
                authorizer.authorizeSchema(sM.getSchemaMetadata().getSchemaGroup(),
                        sM.getSchemaMetadata().getName(),
                        Authorizer.ACCESS_TYPE_READ,
                        getUserNameFromSC(sc),
                        getUserGroupsFromSC(sc)));
    }

    @Override
    public Stream<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext sc,
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
    public Collection<SchemaVersionInfo> authorizeGetAllSchemaVersions
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
    throws SchemaNotFoundException, AuthorizationException {

        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranchName, Authorizer.ACCESS_TYPE_READ);

        return func.get();
    }

    private void authorizeSchemaVersionOpInternal(SecurityContext sc,
                                                  SchemaMetadataInfo schemaMetadataInfo,
                                                  String schemaBranch,
                                                  String accessType) throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String user = getUserNameFromSC(sc);

        boolean hasAccess = authorizer.authorizeSchemaVersion(sGroup,
                sName,
                schemaBranch,
                accessType,
                user,
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccess,
                user,
                accessType,
                Authorizer.RESOURCE_SCHEMA_VERSION,
                compoundNameToString(sGroup, sName, schemaBranch));
    }

    private SchemaBranch getPrimaryBranch(Collection<SchemaBranch> branches) {
        return branches.stream().min((b1, b2) -> b1.getId() < b2.getId() ? -1 :
                b1.getId() > b2.getId() ? 1 : 0).get();
    }

    private String getUserNameFromSC(SecurityContext sc) {
        Principal p = sc.getUserPrincipal();
        KerberosName kerberosName = new KerberosName(p.getName());

        try {
            return kerberosName.getShortName();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> getUserGroupsFromSC(SecurityContext sc) {
        Set<String> res = new HashSet<>();
        String user = getUserNameFromSC(sc);

        List<String> groups = UserGroupInformation.createRemoteUser(user).getGroups();

        res.addAll(groups);

        return res;
    }

    private void raiseAuthorizationExceptionIfNeeded(boolean isAuthorized,
                                                     String user,
                                                     String permission,
                                                     String entityType,
                                                     String entity) throws AuthorizationException {
        if(!isAuthorized) {
            throw new AuthorizationException(String.format(
                    "User [%s] does not have [%s] permissions on [%s]: [%s]", user,
                    permission, entityType.toUpperCase(), entity));
        }
    }

    private static String compoundNameToString(String... elements) {
        if (elements == null) {
            return "";
        }
        return StringUtils.join(elements, "#");
    }

}
