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
import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.Map;

public class DummyAuthorizationAgent implements AuthorizationAgent {


    @Override
    public void configure(Map<String, Object> props) { }

    @Override
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(SecurityContext sc,
                                                               Collection<SchemaMetadataInfo> schemas) {
        return schemas;
    }

    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields(SecurityContext sc,
                                                                     ISchemaRegistry schemaRegistry,
                                                                     Collection<SchemaVersionKey> versions) {
        return versions;
    }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        SchemaMetadata schemaMetadata,
                                        Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaMetadata(SecurityContext sc,
                                        ISchemaRegistry schemaRegistry,
                                        String schemaMetadataName,
                                        Authorizer.AccessType accessType) { }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> authorizeGetAggregatedSchemaList
            (SecurityContext sc,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList) {

        return aggregatedSchemaMetadataInfoList;
    }

    @Override
    public void authorizeGetAggregatedSchemaInfo(SecurityContext sc,
                                                 AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo) { }


    @Override
    public void authorizeGetSerializers(SecurityContext securityContext,
                                        SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public void authorizeDeleteSchemaBranch(SecurityContext securityContext,
                                            ISchemaRegistry schemaRegistry,
                                            Long schemaBranchId) { }

    @Override
    public Collection<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext securityContext,
                                                             ISchemaRegistry schemaRegistry,
                                                             Collection<SchemaVersionInfo> versions){
        return versions;
    }

    @Override
    public void authorizeCreateSchemaBranch(SecurityContext securityContext,
                                            ISchemaRegistry schemaRegistry,
                                            String schemaMetadataName,
                                            Long versionId,
                                            String branchTocreate) { }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext securityContext,
                                                            ISchemaRegistry schemaRegistry,
                                                            String schemaMetadataName,
                                                            Collection<SchemaBranch> branches) {
        return branches;
    }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       String schemaMetadataName,
                                       String schemaBranch,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionKey versionKey,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaVersionInfo versionInfo,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       SchemaIdVersion versionId,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeSchemaVersion(SecurityContext securityContext,
                                       ISchemaRegistry schemaRegistry,
                                       Long versionId,
                                       Authorizer.AccessType accessType) { }

    @Override
    public void authorizeMergeSchemaVersion(SecurityContext securityContext,
                                            ISchemaRegistry schemaRegistry,
                                            Long versionId) { }

    @Override
    public void authorizeSerDes(SecurityContext sc,
                                Authorizer.AccessType accessType) { }

    @Override
    public void authorizeMapSchemaWithSerDes(SecurityContext securityContext,
                                             ISchemaRegistry schemaRegistry,
                                             String schemaMetadataName) { }

}
