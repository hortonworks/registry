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

import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import java.util.Collection;
import java.util.Map;

public class NOOPAuthorizationAgent implements AuthorizationAgent {


    @Override
    public void configure(Map<String, Object> props) { }

    @Override
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(Authorizer.UserAndGroups userAndGroups,
                                                               Collection<SchemaMetadataInfo> schemas) {
        return schemas;
    }

    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields(Authorizer.UserAndGroups userAndGroups,
                                                                     ISchemaRegistry schemaRegistry,
                                                                     Collection<SchemaVersionKey> versions) {
        return versions;
    }

    @Override
    public void authorizeDeleteSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                              ISchemaRegistry schemaRegistry,
                                              String schemaMetadataName) { }

    @Override
    public void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                        SchemaMetadata schemaMetadata,
                                        Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                        ISchemaRegistry schemaRegistry,
                                        String schemaMetadataName,
                                        Authorizer.AccessType accessType) { }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> authorizeGetAggregatedSchemaList
            (Authorizer.UserAndGroups userAndGroups,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList) {

        return aggregatedSchemaMetadataInfoList;
    }

    @Override
    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(Authorizer.UserAndGroups userAndGroups,
                                                                         AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo) {
        return aggregatedSchemaMetadataInfo;
    }


    @Override
    public void authorizeGetSerializers(Authorizer.UserAndGroups userAndGroups,
                                        SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public void authorizeDeleteSchemaBranch(Authorizer.UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            Long schemaBranchId) { }

    @Override
    public Collection<SchemaVersionInfo> authorizeGetAllVersions(Authorizer.UserAndGroups userAndGroups,
                                                             ISchemaRegistry schemaRegistry,
                                                             Collection<SchemaVersionInfo> versions){
        return versions;
    }

    @Override
    public void authorizeCreateSchemaBranch(Authorizer.UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            Long schemaMetadataId,
                                            Long versionId,
                                            String branchTocreate) { }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(Authorizer.UserAndGroups userAndGroups,
                                                            ISchemaRegistry schemaRegistry,
                                                            String schemaMetadataName,
                                                            Collection<SchemaBranch> branches) {
        return branches;
    }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       String schemaMetadataName,
                                       String schemaBranch,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionKey versionKey,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionInfo versionInfo,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaIdVersion versionId,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       Long versionId,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeMergeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                            ISchemaRegistry schemaRegistry,
                                            Long versionId) { }

    @Override
    public void authorizeSerDes(Authorizer.UserAndGroups userAndGroups,
                                Authorizer.AccessType accessType) { }

    @Override
    public void authorizeMapSchemaWithSerDes(Authorizer.UserAndGroups userAndGroups,
                                             ISchemaRegistry schemaRegistry,
                                             String schemaMetadataName) { }

}
