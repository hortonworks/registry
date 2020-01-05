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
package com.hortonworks.registries.schemaregistry.authorizer.agent;

import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;

public enum DefaultAuthorizationAgent implements AuthorizationAgent {
    INSTANCE;

    private Authorizer authorizer;

    @Override
    public void init(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

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
        authorize(schemaMetadataResource, Authorizer.AccessType.READ, sc);

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
             ISchemaRegistry schemaRegistry,
             Collection<SchemaVersionKey> versions) {

        return versions.stream()
                .filter(schemaVersionKey -> {
                    String sName = schemaVersionKey.getSchemaName();
                    String sGroup = schemaRegistry.getSchemaMetadataInfo(sName).getSchemaMetadata().getSchemaGroup();
                    SchemaVersionInfo svi;
                    try {
                        svi = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
                    } catch (SchemaNotFoundException e) {
                       throw new RuntimeException(e);
                    }
                    String sBranch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(svi.getId()));
                    return authorizer.authorize(
                            new Authorizer.SchemaVersionResource(sGroup, sName, sBranch),
                            Authorizer.AccessType.READ,
                            getUserNameFromSC(sc),
                            getUserGroupsFromSC(sc));
                }).collect(Collectors.toList());

    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc, ISchemaRegistry schemaRegistry, String schemaMetadataName, Authorizer.AccessType accessType) throws AuthorizationException {
        authorizeSchemaMetadata(sc,
                schemaRegistry.getSchemaMetadataInfo(schemaMetadataName),
                accessType);
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Authorizer.AccessType accessType) throws AuthorizationException {
        authorizeSchemaMetadata(sc,
                schemaMetadataInfo.getSchemaMetadata(),
                accessType);
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc, SchemaMetadata schemaMetadata,
                                        Authorizer.AccessType accessType) throws AuthorizationException {
        String sGroup = schemaMetadata.getSchemaGroup();
        String sName = schemaMetadata.getName();

        Authorizer.SchemaMetadataResource schemaMetadataResource = new Authorizer.SchemaMetadataResource(sGroup, sName);
        authorize(schemaMetadataResource, accessType, sc);
    }

    @Override
    public void authorizeCreateSchemaBranch(SecurityContext sc,
                                            ISchemaRegistry schemaRegistry,
                                            String schemaMetadataName,
                                            Long schemaVersionId,
                                            String branchTocreate) throws AuthorizationException {
        SchemaMetadata sM = schemaRegistry
                .getSchemaMetadataInfo(schemaMetadataName)
                .getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        Authorizer.SchemaBranchResource schemaBranchResource = new Authorizer.SchemaBranchResource(sGroup, sName, branchTocreate);
        authorize(schemaBranchResource, Authorizer.AccessType.CREATE, sc);

        String branch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(schemaVersionId));
        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        authorize(schemaVersionResource, Authorizer.AccessType.READ, sc);
    }

    @Override
    public void authorizeDeleteSchemaBranch(SecurityContext sc,
                                            ISchemaRegistry schemaRegistry,
                                            Long schemaBranchId)
            throws AuthorizationException {
        SchemaBranch sb = schemaRegistry.getSchemaBranch(schemaBranchId);
        SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(sb.getSchemaMetadataName()).getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String schemaBranchName = sb.getName();

        Authorizer.SchemaBranchResource schemaBranchResource = new Authorizer.SchemaBranchResource(sGroup,
                sName,
                schemaBranchName);
        authorize(schemaBranchResource, Authorizer.AccessType.DELETE, sc);
    }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext sc,
                                                            ISchemaRegistry schemaRegistry,
                                                            String schemaMetadataName,
                                                            Collection<SchemaBranch> branches) {

        return branches.stream().filter(branch -> {
            SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName).getSchemaMetadata();
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
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaIdVersion versionId,
                                       Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {
        authorizeSchemaVersion(securityContext, schemaRegistry, schemaRegistry.getSchemaVersionInfo(versionId), accessType);
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       Long versionId,
                                       Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {
        SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(versionId));
        authorizeSchemaVersion(securityContext, schemaRegistry, schemaVersionInfo, accessType);
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionKey versionKey,
                                       Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        authorizeSchemaVersion(securityContext, schemaRegistry,
                schemaRegistry.getSchemaVersionInfo(versionKey), accessType);
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionInfo versionInfo,
                                       Authorizer.AccessType accessType)
            throws AuthorizationException {

        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(versionInfo.getSchemaMetadataId());
        Collection<SchemaBranch> branches = schemaRegistry.getSchemaBranchesForVersion(versionInfo.getId());
        String schemaBranch = getPrimaryBranch(branches);
        authorizeSchemaVersion(securityContext, schemaMetadataInfo, schemaBranch, accessType);
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext sc,
                                       ISchemaRegistry schemaRegistry,
                                       String schemaMetadataName,
                                       String schemaBranch,
                                       Authorizer.AccessType accessType)
            throws AuthorizationException {

        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName);
        authorizeSchemaVersion(sc, schemaMetadataInfo, schemaBranch, accessType);
    }

    private void authorizeSchemaVersion(SecurityContext sc,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        String schemaBranch,
                                        Authorizer.AccessType accessType)
            throws AuthorizationException {
        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup,
                sName,
                schemaBranch);
        authorize(schemaVersionResource, accessType, sc);
    }

    public void authorizeGetSerializers(SecurityContext sc,
                                        SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException {
        authorizeSchemaMetadata(sc, schemaMetadataInfo, Authorizer.AccessType.READ);
        authorizeSerDes(sc, Authorizer.AccessType.READ);
    }

    public void authorizeSerDes(SecurityContext sc, Authorizer.AccessType accessType) throws AuthorizationException  {
        Authorizer.SerdeResource serdeResource = new Authorizer.SerdeResource();
        authorize(serdeResource, accessType, sc);
    }

    @Override
    public void authorizeMapSchemaWithSerDes(SecurityContext sc,
                                             ISchemaRegistry schemaRegistry,
                                             String schemaMetadataName) throws AuthorizationException {
        authorizeSerDes(sc, Authorizer.AccessType.READ);
        authorizeSchemaMetadata(sc,
                schemaRegistry.getSchemaMetadataInfo(schemaMetadataName).getSchemaMetadata(),
                Authorizer.AccessType.UPDATE);
    }

    @Override
    public void authorizeMergeSchemaVersion(SecurityContext sc,
                                            ISchemaRegistry schemaRegistry,
                                            Long versionId)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(versionId));
        SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(schemaVersionInfo.getSchemaMetadataId()).getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        String branch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(versionId));

        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        authorize(schemaVersionResource, Authorizer.AccessType.READ, sc);

        Authorizer.SchemaVersionResource schemaVersionToCreate =
                new Authorizer.SchemaVersionResource(sGroup, sName, SchemaBranch.MASTER_BRANCH);
        authorize(schemaVersionToCreate, Authorizer.AccessType.CREATE, sc);
    }

    ///////////////////// ConfluenceCompatible APIs /////////////////////

    @Override
    public Stream<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext sc,
                                                             ISchemaRegistry schemaRegistry,
                                                             Stream<SchemaVersionInfo> vStream){
        return vStream.filter(schemaVersionInfo -> {
            SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(schemaVersionInfo.getSchemaMetadataId()).getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();
            String sBranch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(schemaVersionInfo.getId()));

            Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup,
                    sName,
                    sBranch);
            return authorizer.authorize(schemaVersionResource,
                    Authorizer.AccessType.READ,
                    getUserNameFromSC(sc),
                    getUserGroupsFromSC(sc));
        });
    }

    ////////////////////////////////////////////////////////////////////////

    private void authorize(Authorizer.Resource resource, Authorizer.AccessType accessType, SecurityContext sc)
            throws AuthorizationException {

        String user = getUserNameFromSC(sc);
        boolean isAuthorized = authorizer.authorize(resource,
                accessType,
                user,
                getUserGroupsFromSC(sc));

        raiseAuthorizationExceptionIfNeeded(isAuthorized,
                user,
                accessType,
                resource);
    }

    private String getPrimaryBranch(Collection<SchemaBranch> branches) {
        return branches.stream().min((b1, b2) -> b1.getId() < b2.getId() ? -1 :
                b1.getId() > b2.getId() ? 1 : 0).get().getName();
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
