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
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.hadoop.security.authorize.AuthorizationException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public interface AuthorizationAgent {

    void init(Authorizer authorizer);


    Collection<AggregatedSchemaMetadataInfo> authorizeListAggregatedSchemas
            (SecurityContext sc,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList);


    AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(SecurityContext sc,
                                                                  AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo)
            throws AuthorizationException;


    Collection<SchemaMetadataInfo> authorizeFindSchemas(SecurityContext sc,
                                                        Collection<SchemaMetadataInfo> schemas);


    List<AggregatedSchemaMetadataInfo> authorizeFindAggregatedSchemas(SecurityContext sc,
                                                                      List<AggregatedSchemaMetadataInfo> asmi);


    Collection<SchemaVersionKey> authorizeFindSchemasByFields
            (SecurityContext sc,
             ISchemaRegistry schemaRegistry,
             Collection<SchemaVersionKey> versions)
            throws SchemaNotFoundException;


    void authorizeSchemaMetadata(SecurityContext sc, SchemaMetadata schemaMetadata, Authorizer.AccessType accessType) throws AuthorizationException;

    void authorizeSchemaMetadata(SecurityContext sc, SchemaMetadataInfo schemaMetadataInfo, Authorizer.AccessType accessType) throws AuthorizationException;

    void authorizeSchemaMetadata(SecurityContext sc, ISchemaRegistry schemaRegistry,
                                 String schemaMetadataName, Authorizer.AccessType accessType)
            throws AuthorizationException;


    void authorizeCreateSchemaBranch(SecurityContext securityContext,
                                     ISchemaRegistry schemaRegistry,
                                     String schemaMetadataName,
                                     Long versionId,
                                     String branchTocreate)
            throws AuthorizationException;


    void authorizeDeleteSchemaBranch(SecurityContext securityContext,
                                     ISchemaRegistry schemaRegistry,
                                     Long schemaBranchId)
            throws AuthorizationException;


    Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext securityContext,
                                                     ISchemaRegistry schemaRegistry,
                                                     String schemaMetadataName,
                                                     Collection<SchemaBranch> branches)
            throws SchemaNotFoundException;


    void authorizeSchemaVersion(SecurityContext securityContext,
                                ISchemaRegistry schemaRegistry,
                                String schemaMetadataName,
                                String schemaBranch,
                                Authorizer.AccessType accessType)
            throws AuthorizationException;


    void authorizeSchemaVersion(SecurityContext securityContext,
                                ISchemaRegistry schemaRegistry,
                                SchemaVersionKey versionKey,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaVersion(SecurityContext securityContext,
                                ISchemaRegistry schemaRegistry,
                                SchemaVersionInfo versionInfo,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaVersion(SecurityContext securityContext,
                                ISchemaRegistry schemaRegistry,
                                SchemaIdVersion versionId,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaVersion(SecurityContext securityContext,
                                ISchemaRegistry schemaRegistry,
                                Long versionId,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeMergeSchemaVersion(SecurityContext securityContext,
                                     ISchemaRegistry schemaRegistry,
                                     Long versionId)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSerDes(SecurityContext sc,
                         Authorizer.AccessType accessType) throws AuthorizationException;


    void authorizeGetSerializers(SecurityContext securityContext,
                                 SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException;


    void authorizeMapSchemaWithSerDes(SecurityContext securityContext,
                                      ISchemaRegistry schemaRegistry,
                                      String schemaMetadataName) throws AuthorizationException;


    ///////////////// ConfluentCompatible APIs //////////////////////////////

    Stream<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext securityContext,
                                                      ISchemaRegistry schemaRegistry,
                                                      Stream<SchemaVersionInfo> vStream);

}
