/*
 * Copyright 2016-2020 Cloudera, Inc.
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

import com.google.common.annotations.VisibleForTesting;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.security.authorize.AuthorizationException;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer.AccessType;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer.UserAndGroups;

import javax.ws.rs.NotSupportedException;

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
            (UserAndGroups userAndGroups,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList)
            throws SchemaNotFoundException {

        return removeUnauthorizedAndNullEntities(aggregatedSchemaMetadataInfoList,
                aggregatedSchemaMetadataInfo -> authorizeGetAggregatedSchemaInfo(userAndGroups,
                        aggregatedSchemaMetadataInfo));

    }


    @Override
    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(UserAndGroups userAndGroups,
                                                                         AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo)
            throws AuthorizationException, SchemaNotFoundException {
        SchemaMetadata sm = aggregatedSchemaMetadataInfo.getSchemaMetadata();

        authorizeSchemaMetadata(userAndGroups, sm, AccessType.READ);

        Collection<AggregatedSchemaBranch> filteredBranches =
                removeUnauthorizedAndNullEntities(aggregatedSchemaMetadataInfo.getSchemaBranches(),
                        aggregatedBranch -> authorizeGetAggregatedBranch(sm, userAndGroups, aggregatedBranch));

        Collection<SerDesInfo> serDesInfos = aggregatedSchemaMetadataInfo.getSerDesInfos();

        if(serDesInfos != null &&
                !serDesInfos.isEmpty() &&
                !authorizer.authorize(new Authorizer.SerdeResource(), AccessType.READ, userAndGroups)) {
            serDesInfos = new ArrayList<>();
        }

        return new AggregatedSchemaMetadataInfo(sm,
                aggregatedSchemaMetadataInfo.getId(),
                aggregatedSchemaMetadataInfo.getTimestamp(),
                filteredBranches,
                serDesInfos);
    }

    private AggregatedSchemaBranch authorizeGetAggregatedBranch(SchemaMetadata sm,
                                                        UserAndGroups userAndGroups,
                                                        AggregatedSchemaBranch branch)
                throws AuthorizationException {

        String sGroup = sm.getSchemaGroup();
        String sName = sm.getName();
        String bName = branch.getSchemaBranch().getName();

        authorize(new Authorizer.SchemaBranchResource(sGroup, sName, bName),
                AccessType.READ,
                userAndGroups);

        authorize(new Authorizer.SchemaVersionResource(sGroup, sName, bName),
                AccessType.READ,
                userAndGroups);

        return branch;
    }

    @Override
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(UserAndGroups userAndGroups,
                                                               Collection<SchemaMetadataInfo> schemas)
            throws SchemaNotFoundException {

        return authorizeGetEntities(userAndGroups, schemas, schemaMetadataInfo -> {
            SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
            String gName = schemaMetadata.getSchemaGroup();
            String smName = schemaMetadata.getName();

            return new Authorizer.SchemaMetadataResource(gName, smName);
        });
    }


    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields
            (UserAndGroups userAndGroups,
             ISchemaRegistry schemaRegistry,
             Collection<SchemaVersionKey> versions) throws SchemaNotFoundException {

        return authorizeGetEntities(userAndGroups, versions, schemaVersionKey -> {
            String sName = schemaVersionKey.getSchemaName();
            String sGroup = schemaRegistry.getSchemaMetadataInfo(sName).getSchemaMetadata().getSchemaGroup();
            SchemaVersionInfo svi = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
            String sBranch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(svi.getId()));

            return new Authorizer.SchemaVersionResource(sGroup, sName, sBranch);
        });
    }

    @Override
    public void authorizeDeleteSchemaMetadata(UserAndGroups userAndGroups,
                                              ISchemaRegistry schemaRegistry,
                                              String schemaMetadataName)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaMetadataInfo smi = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName);
        if(smi == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaMetadataName);
        }

        Authorizer.SchemaMetadataResource schemaMetadataResource = new Authorizer.SchemaMetadataResource(
                smi.getSchemaMetadata().getSchemaGroup(), schemaMetadataName);
        authorize(schemaMetadataResource, AccessType.DELETE, userAndGroups);

        Collection<SchemaBranch> branches = schemaRegistry.getSchemaBranches(schemaMetadataName);
        if(branches != null) {
            for(SchemaBranch branch : branches) {
                SchemaBranch sb = schemaRegistry.getSchemaBranch(branch.getId());
                authorizeDeleteSchemaBranch(userAndGroups, sb, smi);
            }
        }
    }

    @Override
    public void authorizeSchemaMetadata(UserAndGroups userAndGroups,
                                        ISchemaRegistry schemaRegistry,
                                        String schemaMetadataName,
                                        AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaMetadataInfo smi = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName);
        if(smi == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaMetadataName);
        }
        authorizeSchemaMetadata(userAndGroups,
                smi,
                accessType);
    }

    @Override
    public void authorizeSchemaMetadata(UserAndGroups userAndGroups,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        AccessType accessType)
            throws AuthorizationException {

        authorizeSchemaMetadata(userAndGroups,
                schemaMetadataInfo.getSchemaMetadata(),
                accessType);
    }


    @Override
    public void authorizeSchemaMetadata(UserAndGroups userAndGroups, SchemaMetadata schemaMetadata,
                                         AccessType accessType) throws AuthorizationException {

        String sGroup = schemaMetadata.getSchemaGroup();
        String sName = schemaMetadata.getName();

        Authorizer.SchemaMetadataResource schemaMetadataResource = new Authorizer.SchemaMetadataResource(sGroup, sName);
        authorize(schemaMetadataResource, accessType, userAndGroups);

        if(accessType == AccessType.DELETE) {
            throw new NotSupportedException("AccessType.DELETE is not supported for authorizeSchemaMetadata method");
        }
    }


    @Override
    public void authorizeCreateSchemaBranch(UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            String schemaMetadataName,
                                            Long schemaVersionId,
                                            String branchTocreate)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaMetadata sM = schemaRegistry
                .getSchemaMetadataInfo(schemaMetadataName)
                .getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        Authorizer.SchemaBranchResource schemaBranchResource =
                new Authorizer.SchemaBranchResource(sGroup, sName, branchTocreate);
        authorize(schemaBranchResource, AccessType.CREATE, userAndGroups);

        Collection<SchemaBranch> branchSet = schemaRegistry.getSchemaBranchesForVersion(schemaVersionId);
        if(branchSet.isEmpty()) {
            throw new SchemaNotFoundException("Schema version with id : " + schemaVersionId + " not found");
        }
        String branch = getPrimaryBranch(branchSet);
        Authorizer.SchemaVersionResource schemaVersionResource =
                new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        authorize(schemaVersionResource, AccessType.READ, userAndGroups);
    }


    @Override
    public void authorizeDeleteSchemaBranch(UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            Long schemaBranchId)
            throws AuthorizationException {

        SchemaBranch sb = schemaRegistry.getSchemaBranch(schemaBranchId);
        SchemaMetadataInfo sMi = schemaRegistry.getSchemaMetadataInfo(sb.getSchemaMetadataName());

        authorizeDeleteSchemaBranch(userAndGroups, sb, sMi);
    }

    private void authorizeDeleteSchemaBranch(UserAndGroups userAndGroups,
                                             SchemaBranch schemaBranch,
                                             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException {

        SchemaMetadata sM = schemaMetadataInfo.getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();
        String schemaBranchName = schemaBranch.getName();

        Authorizer.SchemaBranchResource schemaBranchResource = new Authorizer.SchemaBranchResource(sGroup,
                sName,
                schemaBranchName);
        authorize(schemaBranchResource, AccessType.DELETE, userAndGroups);

        Authorizer.SchemaVersionResource schemaVersionResource = new Authorizer.SchemaVersionResource(sGroup,
                sName,
                schemaBranchName);
        authorize(schemaVersionResource, AccessType.DELETE, userAndGroups);

    }


    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(UserAndGroups userAndGroups,
                                                            ISchemaRegistry schemaRegistry,
                                                            String schemaMetadataName,
                                                            Collection<SchemaBranch> branches)
            throws SchemaNotFoundException {

        SchemaMetadataInfo smi = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName);
        if(smi == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaMetadataName);
        }
        SchemaMetadata sm = smi.getSchemaMetadata();
        String sGroup = sm.getSchemaGroup();
        String sName = schemaMetadataName;

        return authorizeGetEntities(userAndGroups, branches, branch ->
                new Authorizer.SchemaBranchResource(sGroup, sName, branch.getName()));
    }


    @Override
    public void authorizeSchemaVersion(UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaIdVersion versionId,
                                       AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        authorizeSchemaVersion(userAndGroups,
                schemaRegistry,
                schemaRegistry.getSchemaVersionInfo(versionId),
                accessType);
    }

    @Override
    public void authorizeSchemaVersion(UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       Long versionId,
                                       AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(versionId));
        authorizeSchemaVersion(userAndGroups, schemaRegistry, schemaVersionInfo, accessType);
    }

    @Override
    public void authorizeSchemaVersion(UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionKey versionKey,
                                       AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        authorizeSchemaVersion(userAndGroups,
                schemaRegistry,
                schemaRegistry.getSchemaVersionInfo(versionKey),
                accessType);
    }

    @Override
    public void authorizeSchemaVersion(UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionInfo versionInfo,
                                       AccessType accessType)
            throws AuthorizationException {

        SchemaMetadataInfo schemaMetadataInfo =
                schemaRegistry.getSchemaMetadataInfo(versionInfo.getSchemaMetadataId());
        Collection<SchemaBranch> branches = schemaRegistry.getSchemaBranchesForVersion(versionInfo.getId());
        String schemaBranch = getPrimaryBranch(branches);
        authorizeSchemaVersion(userAndGroups, schemaMetadataInfo, schemaBranch, accessType);
    }

    @Override
    public void authorizeSchemaVersion(UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       String schemaMetadataName,
                                       String schemaBranch,
                                       AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName);
        if(schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaMetadataName);
        }
        authorizeSchemaVersion(userAndGroups, schemaMetadataInfo, schemaBranch, accessType);
    }

    private void authorizeSchemaVersion(UserAndGroups userAndGroups,
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
        authorize(schemaVersionResource, accessType, userAndGroups);
    }

    public void authorizeGetSerializers(UserAndGroups userAndGroups,
                                        SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException {

        authorizeSchemaMetadata(userAndGroups, schemaMetadataInfo.getSchemaMetadata(), AccessType.READ);
        authorizeSerDes(userAndGroups, AccessType.READ);
    }

    @Override
    public void authorizeSerDes(UserAndGroups userAndGroups, AccessType accessType)
            throws AuthorizationException  {

        Authorizer.SerdeResource serdeResource = new Authorizer.SerdeResource();
        authorize(serdeResource, accessType, userAndGroups);
    }


    @Override
    public void authorizeMapSchemaWithSerDes(UserAndGroups userAndGroups,
                                             ISchemaRegistry schemaRegistry,
                                             String schemaMetadataName)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaMetadataInfo smi = schemaRegistry.getSchemaMetadataInfo(schemaMetadataName);
        if(smi == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaMetadataName);
        }

        authorizeSerDes(userAndGroups, AccessType.READ);
        authorizeSchemaMetadata(userAndGroups, smi.getSchemaMetadata(), AccessType.UPDATE);
    }

    @Override
    public void authorizeMergeSchemaVersion(UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            Long versionId)
            throws AuthorizationException, SchemaNotFoundException {

        SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(versionId));
        SchemaMetadata sM = schemaRegistry.getSchemaMetadataInfo(schemaVersionInfo.getSchemaMetadataId()).getSchemaMetadata();
        String sGroup = sM.getSchemaGroup();
        String sName = sM.getName();

        String branch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(versionId));

        Authorizer.SchemaVersionResource schemaVersionResource =
                new Authorizer.SchemaVersionResource(sGroup, sName, branch);
        authorize(schemaVersionResource, AccessType.READ, userAndGroups);

        Authorizer.SchemaVersionResource schemaVersionToCreate =
                new Authorizer.SchemaVersionResource(sGroup, sName, SchemaBranch.MASTER_BRANCH);
        authorize(schemaVersionToCreate, AccessType.CREATE, userAndGroups);
    }

    @Override
    public Collection<SchemaVersionInfo> authorizeGetAllVersions(UserAndGroups userAndGroups,
                                                                 ISchemaRegistry schemaRegistry,
                                                                 Collection<SchemaVersionInfo> versions)
            throws SchemaNotFoundException {

        return authorizeGetEntities(userAndGroups, versions, schemaVersionInfo -> {
            SchemaMetadata sM =
                    schemaRegistry.getSchemaMetadataInfo(schemaVersionInfo.getSchemaMetadataId())
                            .getSchemaMetadata();
            String sGroup = sM.getSchemaGroup();
            String sName = sM.getName();
            String sBranch = getPrimaryBranch(schemaRegistry.getSchemaBranchesForVersion(schemaVersionInfo.getId()));

            return new Authorizer.SchemaVersionResource(sGroup, sName, sBranch);
        });

    }

    public static class AlreadyConfiguredException extends RuntimeException {
        public AlreadyConfiguredException(String message) {
            super(message);
        }
    }

    private interface EntityFilterFunction<T> {
        T filter(T elem) throws AuthorizationException, SchemaNotFoundException;
    }

    private interface EntityToAuthorizerResourceMapFunc<T> {
        Authorizer.Resource map(T elem) throws SchemaNotFoundException;
    }

    private <T> Collection<T> authorizeGetEntities(UserAndGroups userAndGroups,
                                                   Collection<T> entities,
                                                   EntityToAuthorizerResourceMapFunc<T> mapFunc)
            throws SchemaNotFoundException {
        return removeUnauthorizedAndNullEntities(entities, elem ->
                authorizer.authorize(mapFunc.map(elem), AccessType.READ, userAndGroups) ?
                        elem : null);
    }

    private <T> Collection<T> removeUnauthorizedAndNullEntities(Collection<T> elems,
                                                                EntityFilterFunction<T> filterFunc)
            throws SchemaNotFoundException {
        if(elems == null) {
            return null;
        }
        ArrayList<T> res = new ArrayList<>();
        for(T elem : elems) {
            try {
                T newElem = filterFunc.filter(elem);
                if(newElem != null) {
                    res.add(newElem);
                }
            } catch (AuthorizationException e) { }
        }

        return res;
    }

    private void authorize(Authorizer.Resource resource, AccessType accessType, UserAndGroups userAndGroups)
            throws AuthorizationException {

        boolean isAuthorized = authorizer.authorize(resource, accessType, userAndGroups);

        raiseAuthorizationExceptionIfNeeded(isAuthorized,
                userAndGroups.getUser(),
                accessType,
                resource);
    }

    private String getPrimaryBranch(Collection<SchemaBranch> branches) {
        return branches.stream().min(Comparator.comparing(SchemaBranch::getId)).get().getName();
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
    @VisibleForTesting
    Authorizer getAuthorizer() {
        return authorizer;
    }

}
