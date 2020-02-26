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

import java.util.Collection;
import java.util.Map;

/**
 * This is the main authorization interface that is implemented by {@link NOOPAuthorizationAgent}
 * and {@link DefaultAuthorizationAgent} to provide authorization logic that doesn't depend on authorizaer type.
 * The user can define its own authorization semantic by implementing {@link AuthorizationAgent} interface.
 */
public interface AuthorizationAgent {

    /**
     * Schema Registry config property name that is used to specify AuthorizerAgent class name.
     */
    String AUTHORIZATION_AGENT_CONFIG = "authorizationAgentClassName";

    /**
     * This method is used to perform initial configuration of {@link AuthorizationAgent}
     *
     * @param props - properties from the 'authorization' section of Schema Registry config file.
     */
    void configure(Map<String, Object> props);


    //////// The below methods are the main methods of AuthorizationAgent. They define the athorization logic ////////

    Collection<AggregatedSchemaMetadataInfo> authorizeGetAggregatedSchemaList(Authorizer.UserAndGroups userAndGroups,
                                                                              Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList)
            throws SchemaNotFoundException;


    AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(Authorizer.UserAndGroups userAndGroups,
                                                                  AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo)
            throws AuthorizationException, SchemaNotFoundException;


    Collection<SchemaMetadataInfo> authorizeFindSchemas(Authorizer.UserAndGroups userAndGroups,
                                                        Collection<SchemaMetadataInfo> schemas)
            throws SchemaNotFoundException;


    Collection<SchemaVersionKey> authorizeFindSchemasByFields(Authorizer.UserAndGroups userAndGroups,
                                                              ISchemaRegistry schemaRegistry,
                                                              Collection<SchemaVersionKey> versions)
            throws SchemaNotFoundException;


    void authorizeDeleteSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                       ISchemaRegistry schemaRegistry,
                                       String schemaMetadataName)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                 SchemaMetadata schemaMetadata,
                                 Authorizer.AccessType accessType)
            throws AuthorizationException;


    void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                 SchemaMetadataInfo schemaMetadataInfo,
                                 Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaMetadata(Authorizer.UserAndGroups userAndGroups,
                                 ISchemaRegistry schemaRegistry,
                                 String schemaMetadataName,
                                 Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeCreateSchemaBranch(Authorizer.UserAndGroups userAndGroups,
                                     ISchemaRegistry schemaRegistry,
                                     String schemaMetadataName,
                                     Long versionId,
                                     String branchTocreate)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeDeleteSchemaBranch(Authorizer.UserAndGroups userAndGroups,
                                     ISchemaRegistry schemaRegistry,
                                     Long schemaBranchId)
            throws AuthorizationException;


    Collection<SchemaBranch> authorizeGetAllBranches(Authorizer.UserAndGroups userAndGroups,
                                                     ISchemaRegistry schemaRegistry,
                                                     String schemaMetadataName,
                                                     Collection<SchemaBranch> branches)
            throws SchemaNotFoundException;


    void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                ISchemaRegistry schemaRegistry,
                                String schemaMetadataName,
                                String schemaBranch,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                ISchemaRegistry schemaRegistry,
                                SchemaVersionKey versionKey,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                ISchemaRegistry schemaRegistry,
                                SchemaVersionInfo versionInfo,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                ISchemaRegistry schemaRegistry,
                                SchemaIdVersion versionId,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                ISchemaRegistry schemaRegistry,
                                Long versionId,
                                Authorizer.AccessType accessType)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeMergeSchemaVersion(Authorizer.UserAndGroups userAndGroups,
                                     ISchemaRegistry schemaRegistry,
                                     Long versionId)
            throws AuthorizationException, SchemaNotFoundException;


    void authorizeSerDes(Authorizer.UserAndGroups userAndGroups,
                         Authorizer.AccessType accessType)
            throws AuthorizationException;


    void authorizeGetSerializers(Authorizer.UserAndGroups userAndGroups,
                                 SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException;


    void authorizeMapSchemaWithSerDes(Authorizer.UserAndGroups userAndGroups,
                                      ISchemaRegistry schemaRegistry,
                                      String schemaMetadataName)
            throws AuthorizationException, SchemaNotFoundException;


    Collection<SchemaVersionInfo> authorizeGetAllVersions(Authorizer.UserAndGroups userAndGroups,
                                                          ISchemaRegistry schemaRegistry,
                                                          Collection<SchemaVersionInfo> versions)
            throws SchemaNotFoundException;

}
