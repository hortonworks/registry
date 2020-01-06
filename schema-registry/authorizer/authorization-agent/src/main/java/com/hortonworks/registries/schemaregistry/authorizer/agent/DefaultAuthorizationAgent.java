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
import com.hortonworks.registries.schemaregistry.authorizer.AuthorizerFactory;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer.AccessType;

public class DefaultAuthorizationAgent implements AuthorizationAgent {

    private Authorizer authorizer;



    @Override
    public void configure(Map<String, Object> props) {
        synchronized (DefaultAuthorizationAgent.class) {
            if(authorizer != null) {
                throw new AlreadyConfiguredException("DefaultAuthorizationAgent is already configured");
            }
            this.authorizer = AuthorizerFactory.getAuthorizer(props);
        }
    }



    @Override
    public Collection<AggregatedSchemaMetadataInfo> authorizeGetAggregatedSchemaList
            (SecurityContext sc,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList) {

        return removeUnauthorizedAndNullEntities(aggregatedSchemaMetadataInfoList,
                aggregatedSchemaMetadataInfo -> authorizeGetAggregatedSchemaInfo(sc, aggregatedSchemaMetadataInfo));
    }

    @Override
    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(SecurityContext sc,
                                                                         AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo)
            throws AuthorizationException {

        String user = getUserNameFromSC(sc);
        Set<String> userGroups = getUserGroupsFromSC(sc);

        return authorizeGetAggregatedSchemaInfo(user, userGroups, aggregatedSchemaMetadataInfo);
    }

    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(String user,
                                                                         Set<String> userGroups,
                                                                         AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo)
            throws AuthorizationException {
        String sGroup = aggregatedSchemaMetadataInfo.getSchemaMetadata().getSchemaGroup();
        String sName = aggregatedSchemaMetadataInfo.getSchemaMetadata().getName();

        Authorizer.SchemaMetadataResource schemaMetadataResource = new Authorizer.SchemaMetadataResource(sGroup, sName);
        authorize(schemaMetadataResource, AccessType.READ, user, userGroups);

        Collection<AggregatedSchemaBranch> filteredBranches =
                removeUnauthorizedAndNullEntities(aggregatedSchemaMetadataInfo.getSchemaBranches(),
                        branch -> filterAggregatedBranch(sGroup, sName, user, userGroups, branch));

        Collection<SerDesInfo> serDesInfos = authorizer.authorize(new Authorizer.SerdeResource(),
                AccessType.READ,
                user,
                userGroups) ? aggregatedSchemaMetadataInfo.getSerDesInfos() : new ArrayList<>();


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

        String bName = branch.getSchemaBranch().getName();
        Authorizer.Resource bResource = new Authorizer.SchemaBranchResource(sGroup, sGroup, bName);
        if (!authorizer.authorize(bResource, AccessType.READ, user, userGroups)) {
            return null;
        }

        Authorizer.Resource version = new Authorizer.SchemaVersionResource(sGroup, sName, bName);
        Collection<SchemaVersionInfo> filteredVersions =
                authorizer.authorize(version,
                        AccessType.READ,
                        user,
                        userGroups) ? branch.getSchemaVersionInfos() : new ArrayList<>();

        return new AggregatedSchemaBranch(branch.getSchemaBranch(),
                branch.getRootSchemaVersion(),
                filteredVersions);
    }

    @Override
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(SecurityContext sc,
                                                               Collection<SchemaMetadataInfo> schemas) {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);
        return authorizeGetEntities(user, uGroups, schemas, schemaMetadataInfo -> {
            SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
            String gName = schemaMetadata.getSchemaGroup();
            String smName = schemaMetadata.getName();

            return new Authorizer.SchemaMetadataResource(gName, smName);
        });
    }

    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields
            (SecurityContext sc,
             ISchemaRegistry schemaRegistry,
             Collection<SchemaVersionKey> versions) {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        return authorizeGetEntities(user, uGroups, versions, schemaVersionKey -> {
            String sName = schemaVersionKey.getSchemaName();
            String sGroup = schemaRegistry.getSchemaMetadataInfo(sName).getSchemaMetadata().getSchemaGroup();
            SchemaVersionInfo svi;
            try {
                svi = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
            } catch (SchemaNotFoundException e) {
                throw new RuntimeException(e);
            }
            String sBranch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(svi.getId()));

            return new Authorizer.SchemaVersionResource(sGroup, sName, sBranch);
        });
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        ISchemaRegistry schemaRegistry,
                                        String schemaMetadataName,
                                        AccessType accessType)
            throws AuthorizationException {

        authorizeSchemaMetadata(sc,
                schemaRegistry.getSchemaMetadataInfo(schemaMetadataName),
                accessType);
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        AccessType accessType)
            throws AuthorizationException {

        authorizeSchemaMetadata(sc,
                schemaMetadataInfo.getSchemaMetadata(),
                accessType);
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc, SchemaMetadata schemaMetadata,
                                        AccessType accessType) throws AuthorizationException {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        authorizeSchemaMetadata(user, uGroups, schemaMetadata, accessType);
    }

    private void authorizeSchemaMetadata(String user, Set<String> uGroups, SchemaMetadata schemaMetadata,
                                         AccessType accessType) throws AuthorizationException {

        String sGroup = schemaMetadata.getSchemaGroup();
        String sName = schemaMetadata.getName();

        Authorizer.SchemaMetadataResource schemaMetadataResource = new Authorizer.SchemaMetadataResource(sGroup, sName);
        authorize(schemaMetadataResource, accessType, user, uGroups);
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

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        Authorizer.SchemaBranchResource schemaBranchResource = new Authorizer.SchemaBranchResource(sGroup, sName, branchTocreate);
        authorize(schemaBranchResource, AccessType.CREATE, user, uGroups);

        String branch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(schemaVersionId));
        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        authorize(schemaVersionResource, AccessType.READ, user, uGroups);
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
        authorize(schemaBranchResource, AccessType.DELETE, sc);
    }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext sc,
                                                            ISchemaRegistry schemaRegistry,
                                                            String schemaMetadataName,
                                                            Collection<SchemaBranch> branches) {
        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        return authorizeGetEntities(user, uGroups, branches, branch -> {
            SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName).getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();

            return new Authorizer.SchemaBranchResource(sGroup, sName, branch.getName());
        });
    }


    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaIdVersion versionId,
                                       AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        authorizeSchemaVersion(securityContext,
                schemaRegistry,
                schemaRegistry.getSchemaVersionInfo(versionId),
                accessType);
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       Long versionId,
                                       AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(versionId));
        authorizeSchemaVersion(securityContext, schemaRegistry, schemaVersionInfo, accessType);
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionKey versionKey,
                                       AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        authorizeSchemaVersion(securityContext,
                schemaRegistry,
                schemaRegistry.getSchemaVersionInfo(versionKey),
                accessType);
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionInfo versionInfo,
                                       AccessType accessType)
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
                                       AccessType accessType)
            throws AuthorizationException {

        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName);
        authorizeSchemaVersion(sc, schemaMetadataInfo, schemaBranch, accessType);
    }

    private void authorizeSchemaVersion(SecurityContext sc,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        String schemaBranch,
                                        AccessType accessType)
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

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        authorizeSchemaMetadata(user, uGroups, schemaMetadataInfo.getSchemaMetadata(), AccessType.READ);
        authorizeSerDes(user, uGroups, AccessType.READ);
    }

    public void authorizeSerDes(SecurityContext sc, AccessType accessType) throws AuthorizationException  {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        authorizeSerDes(user, uGroups, accessType);
    }

    private void authorizeSerDes(String user, Set<String> uGroups, AccessType accessType) throws AuthorizationException  {

        Authorizer.SerdeResource serdeResource = new Authorizer.SerdeResource();
        authorize(serdeResource, accessType, user, uGroups);
    }

    @Override
    public void authorizeMapSchemaWithSerDes(SecurityContext sc,
                                             ISchemaRegistry schemaRegistry,
                                             String schemaMetadataName) throws AuthorizationException {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        authorizeSerDes(user, uGroups, AccessType.READ);
        authorizeSchemaMetadata(user,
                uGroups,
                schemaRegistry.getSchemaMetadataInfo(schemaMetadataName).getSchemaMetadata(),
                AccessType.UPDATE);
    }

    @Override
    public void authorizeMergeSchemaVersion(SecurityContext sc,
                                            ISchemaRegistry schemaRegistry,
                                            Long versionId)
            throws AuthorizationException, SchemaNotFoundException {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(versionId));
        SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(schemaVersionInfo.getSchemaMetadataId()).getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        String branch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(versionId));

        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        authorize(schemaVersionResource, AccessType.READ, user, uGroups);

        Authorizer.SchemaVersionResource schemaVersionToCreate =
                new Authorizer.SchemaVersionResource(sGroup, sName, SchemaBranch.MASTER_BRANCH);
        authorize(schemaVersionToCreate, AccessType.CREATE, user, uGroups);
    }

    ///////////////////// ConfluenceCompatible APIs /////////////////////

    @Override
    public Collection<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext sc,
                                                                 ISchemaRegistry schemaRegistry,
                                                                 Collection<SchemaVersionInfo> versions) {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);

        return authorizeGetEntities(user, uGroups, versions, schemaVersionInfo -> {
            SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(schemaVersionInfo.getSchemaMetadataId()).getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();
            String sBranch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(schemaVersionInfo.getId()));

            return new Authorizer.SchemaVersionResource(sGroup, sName, sBranch);
        });

    }

    ////////////////////////////////////////////////////////////////////////

    public static class AlreadyConfiguredException extends RuntimeException {
        public AlreadyConfiguredException(String message) {
            super(message);
        }
    }

    private interface EntityFilterFunction<T> {
        T filter(T elem) throws AuthorizationException;
    }

    private interface EntityToAuthorizerResourceMapFunc<T> {
        Authorizer.Resource map(T elem);
    }

    private <T> Collection<T> authorizeGetEntities(String user,
                                                   Set<String> uGroups,
                                                   Collection<T> entities,
                                                   EntityToAuthorizerResourceMapFunc<T> mapFunc) {
        return removeUnauthorizedAndNullEntities(entities, elem ->
                authorizer.authorize(mapFunc.map(elem),
                        AccessType.READ,
                        user,
                        uGroups) ? elem : null);
    }

    private <T> Collection<T> removeUnauthorizedAndNullEntities(Collection<T> elems,
                                                                EntityFilterFunction<T> filterFunc) {
        return elems
                .stream().map(elem -> {
                    try {
                        return filterFunc.filter(elem);
                    } catch (AuthorizationException e) {
                        return null;
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());

    }


    private void authorize(Authorizer.Resource resource, AccessType accessType, SecurityContext sc)
            throws AuthorizationException {

        String user = getUserNameFromSC(sc);
        Set<String> uGroups = getUserGroupsFromSC(sc);
        authorize(resource, accessType, user, uGroups);
    }

    private void authorize(Authorizer.Resource resource,
                           AccessType accessType,
                           String user,
                           Set<String> uGroups)
            throws AuthorizationException {

        boolean isAuthorized = authorizer.authorize(resource,
                accessType,
                user,
                uGroups);

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
                                                     AccessType permission,
                                                     Authorizer.Resource resource) throws AuthorizationException {
        if(!isAuthorized) {
            throw new AuthorizationException(String.format(
                    "User '%s' does not have [%s] permission on %s", user,
                    permission.getName(), resource));
        }
    }

    /// This method is needed only for the unit tests
    Authorizer getAuthorizer() {
        return authorizer;
    }

}
