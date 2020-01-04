/*
 * Copyright 2016-2019 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        Authorizer.SchemaMetadataResource schemaMetadataResource = new Authorizer.SchemaMetadataResource(sGroup, sName);
        boolean accessToSchemaMetadata = authorizer.authorize(schemaMetadataResource,
                Authorizer.AccessType.READ,
                user,
                userGroups);

        raiseAuthorizationExceptionIfNeeded(accessToSchemaMetadata,
                user,
                Authorizer.AccessType.READ,
                schemaMetadataResource);
        Collection<AggregatedSchemaBranch> filteredBranches = aggregatedSchemaMetadataInfo
                .getSchemaBranches()
                .stream().map(branch -> filterAggregatedBranch(sGroup, sName, user, userGroups, branch))
                .filter(Objects::nonNull).collect(Collectors.toList());

        boolean accessToSerdes = authorizer.authorize(new Authorizer.SerdeResource(),
                Authorizer.AccessType.READ,
                user,
                userGroups);

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


        boolean accessToBranch = authorizer.authorize(
                new Authorizer.SchemaBranchResource(sGroup, sGroup, branch.getSchemaBranch().getName()),
                Authorizer.AccessType.READ,
                user,
                userGroups);
        if (!accessToBranch) {
            return null;
        }

        Collection<SchemaVersionInfo> filteredVersions = branch
                .getSchemaVersionInfos()
                .stream()
                .filter(version -> authorizer.authorize(
                        new Authorizer.SchemaVersionResource(sGroup, sName, branch.getSchemaBranch().getName()),
                        Authorizer.AccessType.READ,
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
                    return authorizer.authorize(new Authorizer.SchemaMetadataResource(sGroup, sName),
                            Authorizer.AccessType.READ,
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
             FunctionWithBranchSchemaNotFoundException<Long, Collection<SchemaBranch>> getVersionBranchesFunc,
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
                    String sBranch = getPrimaryBranch(getVersionBranchesFunc.apply(svi.getId())).getName();
                    return authorizer.authorize(
                            new Authorizer.SchemaVersionResource(sGroup, sName, sBranch),
                            Authorizer.AccessType.READ,
                            getUserNameFromSC(sc),
                            getUserGroupsFromSC(sc));
                }).collect(Collectors.toList());

    }

    @Override
    public void authorizeAddSchemaInfo(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException {
        authorizeSchema(sc, schemaMetadata, Authorizer.AccessType.CREATE);
    }

    @Override
    public void authorizeUpdateSchemaInfo(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException {
        authorizeSchema(sc, schemaMetadata, Authorizer.AccessType.UPDATE);
    }

    @Override
    public SchemaMetadataInfo authorizeGetSchemaInfo
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException {

        authorizeSchema(sc, schemaMetadataInfo.getSchemaMetadata(), Authorizer.AccessType.READ);

        return schemaMetadataInfo;
    }

    @Override
    public void authorizeDeleteSchemaMetadata
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws  AuthorizationException {
        authorizeSchema(sc, schemaMetadataInfo.getSchemaMetadata(), Authorizer.AccessType.DELETE);
    }

    private void authorizeSchema(SecurityContext sc,
                                 SchemaMetadata sM,
                                 Authorizer.AccessType accessType)
            throws  AuthorizationException {
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String user = getUserNameFromSC(sc);

        Authorizer.SchemaMetadataResource schemaMetadataResource = new Authorizer.SchemaMetadataResource(sGroup, sName);
        boolean hasAccess = authorizer.authorize(
                schemaMetadataResource,
                accessType,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(hasAccess,
                user,
                accessType,
                schemaMetadataResource);
    }

    @Override
    public void authorizeAddSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.AccessType.CREATE);
    }

    @Override
    public void authorizeGetLatestSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.AccessType.READ);
    }

    @Override
    public void authorizeGetSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.AccessType.READ);
    }


    @Override
    public void authorizeVersionStateOperation(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.AccessType.UPDATE);
    }

    @Override
    public void authorizeCheckCompatibilityWithSchema(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranch, Authorizer.AccessType.READ);
    }

    @Override
    public void authorizeGetSerializers(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.AccessType.READ);
    }

    @Override
    public void authorizeUploadFile(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.AccessType.UPDATE);
    }

    @Override
    public void authorizeDownloadFile(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.AccessType.READ);
    }

    @Override
    public void authorizeAddSerDes(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.AccessType.CREATE);
    }

    @Override
    public void authorizeGetSerDes(SecurityContext sc) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.AccessType.READ);
    }

    private void authorizeSerDes(SecurityContext sc, Authorizer.AccessType accessType) throws AuthorizationException  {
        String user = getUserNameFromSC(sc);
        Authorizer.SerdeResource serdeResource = new Authorizer.SerdeResource();
        boolean hasAccessToSerdes = authorizer.authorize(serdeResource,
                accessType,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(hasAccessToSerdes,
                user,
                accessType,
                serdeResource);
    }

    @Override
    public void authorizeMapSchemaWithSerDes(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.AccessType.READ);
        authorizeSchema(sc, schemaMetadataInfo.getSchemaMetadata(), Authorizer.AccessType.UPDATE);
    }

    @Override
    public void authorizeDeleteSchemaVersion(SecurityContext sc,
                                             SchemaMetadataInfo schemaMetadataInfo,
                                             Collection<SchemaBranch> branches) throws AuthorizationException {
        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, getPrimaryBranch(branches).getName(), Authorizer.AccessType.DELETE);
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

            return authorizer.authorize(
                    new Authorizer.SchemaBranchResource(sGroup, sName, branch.getName()),
                    Authorizer.AccessType.READ,
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

        Authorizer.SchemaBranchResource schemaBranchResource = new Authorizer.SchemaBranchResource(sGroup, sName, branchTocreate);
        boolean permsToCreateBranches = authorizer.authorize(schemaBranchResource,
                Authorizer.AccessType.CREATE,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToCreateBranches,
                user,
                Authorizer.AccessType.CREATE,
                schemaBranchResource);

        String branch = getPrimaryBranch(branches).getName();
        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        boolean permsToReadVersions= authorizer.authorize(schemaVersionResource,
                Authorizer.AccessType.READ,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToReadVersions,
                user,
                Authorizer.AccessType.READ,
                schemaVersionResource);
    }

    @Override
    public void authorizeMergeSchemaVersion(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Collection<SchemaBranch> branches)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String user = getUserNameFromSC(sc);

        String branch = getPrimaryBranch(branches).getName();
        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        boolean permsToReadVersions = authorizer.authorize(schemaVersionResource,
                Authorizer.AccessType.READ,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToReadVersions,
                user,
                Authorizer.AccessType.READ,
                schemaVersionResource);

        Authorizer.SchemaVersionResource schemaVersionToCreate =
                new Authorizer.SchemaVersionResource(sGroup, sName, SchemaBranch.MASTER_BRANCH);
        boolean permsToCreateVersions = authorizer.authorize(schemaVersionToCreate,
                Authorizer.AccessType.CREATE,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToCreateVersions,
                user,
                Authorizer.AccessType.CREATE,
                schemaVersionToCreate);
    }

    @Override
    public void authorizeDeleteSchemaBranch(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, String schemaBranch)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        String user = getUserNameFromSC(sc);
        Authorizer.SchemaBranchResource schemaBranchResource = new Authorizer.SchemaBranchResource(sGroup,
                sName,
                schemaBranch);
        boolean permsToDelete = authorizer.authorize(schemaBranchResource,
                Authorizer.AccessType.DELETE,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(permsToDelete,
                user,
                Authorizer.AccessType.DELETE,
                schemaBranchResource);
    }

    ///////////////////// ConfluenceCompatible APIs /////////////////////

    @Override
    public Stream<SchemaMetadataInfo> authorizeGetSubjects(SecurityContext sc, Stream<SchemaMetadataInfo> stream) {
        return stream.filter(sM ->
                authorizer.authorize(
                        new Authorizer.SchemaMetadataResource(sM.getSchemaMetadata().getSchemaGroup(),
                                sM.getSchemaMetadata().getName()),
                        Authorizer.AccessType.READ,
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

            Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup,
                    sName,
                    sBranch);
            return authorizer.authorize(schemaVersionResource,
                    Authorizer.AccessType.READ,
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

        authorizeSchemaVersionOpInternal(sc, schemaMetadataInfo, schemaBranchName, Authorizer.AccessType.READ);

        return func.get();
    }

    private void authorizeSchemaVersionOpInternal(SecurityContext sc,
                                                  SchemaMetadataInfo schemaMetadataInfo,
                                                  String schemaBranch,
                                                  Authorizer.AccessType accessType) throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String user = getUserNameFromSC(sc);

        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup,
                sName,
                schemaBranch);
        boolean hasAccess = authorizer.authorize(schemaVersionResource,
                accessType,
                user,
                getUserGroupsFromSC(sc));
        raiseAuthorizationExceptionIfNeeded(hasAccess,
                user,
                accessType,
                schemaVersionResource);
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
                                                     Authorizer.AccessType permission,
                                                     Authorizer.Resource resource) throws AuthorizationException {
        if(!isAuthorized) {
            throw new AuthorizationException(String.format(
                    "User [%s] does not have [%s] permissions on %s", user,
                    permission.getName(), resource));
        }
    }

}
