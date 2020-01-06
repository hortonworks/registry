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
package com.hortonworks.registries.schemaregistry.authorizer.test;

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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class FakeAuthorizationAgent implements AuthorizationAgent {
    @Override
    public void init(Authorizer authorizer) { }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> authorizeListAggregatedSchemas
            (SecurityContext sc, Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList) {
        throw new NotImplementedException();
    }

    @Override
    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo
            (SecurityContext sc, 
             AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo)   {
        throw new NotImplementedException();
    }

    @Override
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(SecurityContext sc,
                                                               Collection<SchemaMetadataInfo> schemas) {
        throw new NotImplementedException();
    }

    @Override
    public List<AggregatedSchemaMetadataInfo> authorizeFindAggregatedSchemas(SecurityContext sc,
                                                                             List<AggregatedSchemaMetadataInfo> asmi) {
        throw new NotImplementedException();
    }

    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields(SecurityContext sc,
                                                                     ISchemaRegistry schemaRegistry,
                                                                     Collection<SchemaVersionKey> versions) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        SchemaMetadata schemaMetadata,
                                        Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        ISchemaRegistry schemaRegistry,
                                        String schemaMetadataName,
                                        Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeCreateSchemaBranch(SecurityContext securityContext,
                                            ISchemaRegistry schemaRegistry,
                                            String schemaMetadataName,
                                            Long versionId,
                                            String branchTocreate)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeDeleteSchemaBranch(SecurityContext securityContext,
                                            ISchemaRegistry schemaRegistry,
                                            Long schemaBranchId)   {
        throw new NotImplementedException();
    }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext securityContext,
                                                            ISchemaRegistry schemaRegistry,
                                                            String schemaMetadataName,
                                                            Collection<SchemaBranch> branches)  {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       String schemaMetadataName,
                                       String schemaBranch,
                                       Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionKey versionKey,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionInfo versionInfo,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaIdVersion versionId,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       Long versionId,
                                       Authorizer.AccessType accessType) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeMergeSchemaVersion(SecurityContext securityContext,
                                            ISchemaRegistry schemaRegistry,
                                            Long versionId) {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeSerDes(SecurityContext sc,
                                Authorizer.AccessType accessType)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeGetSerializers(SecurityContext securityContext,
                                        SchemaMetadataInfo schemaMetadataInfo)   {
        throw new NotImplementedException();
    }

    @Override
    public void authorizeMapSchemaWithSerDes(SecurityContext securityContext,
                                             ISchemaRegistry schemaRegistry,
                                             String schemaMetadataName)   {
        throw new NotImplementedException();
    }

    @Override
    public Stream<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext securityContext,
                                                             ISchemaRegistry schemaRegistry,
                                                             Stream<SchemaVersionInfo> vStream) {
        throw new NotImplementedException();
    }
}
