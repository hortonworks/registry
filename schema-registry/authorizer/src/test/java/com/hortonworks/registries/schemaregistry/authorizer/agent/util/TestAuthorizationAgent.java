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
package com.hortonworks.registries.schemaregistry.authorizer.agent.util;

import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.exception.AuthorizationException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
import java.util.Map;

public class TestAuthorizationAgent implements AuthorizationAgent {
    @Override
    public void configure(Map<String, Object> props) { }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> authorizeGetAggregatedSchemaList
            (Authorizer.UserAndGroups userAndGroups, Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList) {
        throw new NotImplementedException();
    }

    @Override
    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo
            (Authorizer.UserAndGroups userAndGroups, 
             AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo) {
        throw new NotImplementedException();
    }

    @Override
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(Authorizer.UserAndGroups userAndGroups,
                                                               Collection<SchemaMetadataInfo> schemas) {
        throw new NotImplementedException();
    }

    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields(Authorizer.UserAndGroups userAndGroups,
                                                                     ISchemaRegistry schemaRegistry,
                                                                     Collection<SchemaVersionKey> versions) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeDeleteSchemaMetadata(Authorizer.UserAndGroups userAndGroups, ISchemaRegistry schemaRegistry, String schemaMetadataName) throws AuthorizationException, SchemaNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                        SchemaMetadata schemaMetadata,
                                        Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                        ISchemaRegistry schemaRegistry,
                                        String schemaMetadataName,
                                        Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeCreateSchemaBranch(Authorizer.UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            String schemaMetadataName,
                                            Long versionId,
                                            String branchTocreate)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeDeleteSchemaBranch(Authorizer.UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            Long schemaBranchId)   {
        throw new NotImplementedException();
    }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(Authorizer.UserAndGroups userAndGroups,
                                                            ISchemaRegistry schemaRegistry,
                                                            String schemaMetadataName,
                                                            Collection<SchemaBranch> branches)  {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       String schemaMetadataName,
                                       String schemaBranch,
                                       Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionKey versionKey,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionInfo versionInfo,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaIdVersion versionId,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       Long versionId,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeMergeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            Long versionId) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSerDes(Authorizer.UserAndGroups userAndGroups,
                                Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeGetSerializers(Authorizer.UserAndGroups userAndGroups,
                                        SchemaMetadataInfo schemaMetadataInfo)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeMapSchemaWithSerDes(Authorizer.UserAndGroups userAndGroups,
                                             ISchemaRegistry schemaRegistry,
                                             String schemaMetadataName)   {
        throw new NotImplementedException();
    }

    @Override
    public Collection<SchemaVersionInfo> authorizeGetAllVersions(Authorizer.UserAndGroups userAndGroups,
                                                             ISchemaRegistry schemaRegistry,
                                                                 Collection<SchemaVersionInfo> versions) {
        throw new NotImplementedException();
    }

}
