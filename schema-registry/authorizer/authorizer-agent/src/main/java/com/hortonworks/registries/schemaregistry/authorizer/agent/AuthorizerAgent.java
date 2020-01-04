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
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.hadoop.security.authorize.AuthorizationException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface AuthorizerAgent {

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
             Function<String, SchemaMetadataInfo> getSchemaMetadataFunc,
             FunctionWithSchemaNotFoundException<SchemaVersionKey, SchemaVersionInfo> getVersionInfoFunc,
             FunctionWithBranchSchemaNotFoundException<Long, Collection<SchemaBranch>> getVersionBranchesFunc,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionKey>> func)
            throws SchemaNotFoundException;

    void authorizeAddSchemaInfo(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException;

    void authorizeUpdateSchemaInfo(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException;

    SchemaMetadataInfo authorizeGetSchemaInfo
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException;

    Collection<SchemaVersionInfo> authorizeGetAllSchemaVersions
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
            throws SchemaNotFoundException, AuthorizationException;

    void authorizeDeleteSchemaMetadata
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException;


    void authorizeAddSchemaVersion(SecurityContext securityContext,
                                   SchemaMetadataInfo schemaMetadataInfo,
                                   String schemaBranch)
            throws AuthorizationException;

    void authorizeGetLatestSchemaVersion(SecurityContext securityContext,
                                         SchemaMetadataInfo schemaMetadataInfo,
                                         String schemaBranch)
        throws AuthorizationException;

    void authorizeGetSchemaVersion(SecurityContext securityContext,
                                   SchemaMetadataInfo schemaMetadataInfo,
                                   Collection<SchemaBranch> branches)
            throws AuthorizationException;


    void authorizeVersionStateOperation(SecurityContext securityContext,
                                        SchemaMetadataInfo schemaMetadataInfo,
                                        Collection<SchemaBranch> branches)
            throws AuthorizationException;

    void authorizeCheckCompatibilityWithSchema(SecurityContext securityContext,
                                               SchemaMetadataInfo schemaMetadataInfo,
                                               String schemaBranch)
            throws AuthorizationException;

    void authorizeGetSerializers(SecurityContext securityContext,
                                 SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException;

    void authorizeUploadFile(SecurityContext securityContext)  throws AuthorizationException;

    void authorizeDownloadFile(SecurityContext securityContext)  throws AuthorizationException;

    void authorizeAddSerDes(SecurityContext securityContext)  throws AuthorizationException;

    void authorizeGetSerDes(SecurityContext securityContext)  throws AuthorizationException;

    void authorizeMapSchemaWithSerDes(SecurityContext securityContext,
                                      SchemaMetadataInfo schemaMetadataInfo) throws AuthorizationException;

    void authorizeDeleteSchemaVersion(SecurityContext securityContext,
                                      SchemaMetadataInfo schemaMetadataInfo,
                                      Collection<SchemaBranch> branches)
            throws AuthorizationException;

    Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext securityContext,
                                                     SchemaMetadataInfo schemaMetadataInfo,
                                                     SupplierWithSchemaNotFoundException<Collection<SchemaBranch>> func)
            throws SchemaNotFoundException;

    void authorizeCreateSchemaBranch(SecurityContext securityContext,
                                     SchemaMetadataInfo schemaMetadataInfo,
                                     Collection<SchemaBranch> branches,
                                     String branchTocreate)
            throws AuthorizationException;

    void authorizeMergeSchemaVersion(SecurityContext securityContext,
                                     SchemaMetadataInfo schemaMetadataInfo,
                                     Collection<SchemaBranch> schemaBranches)
            throws AuthorizationException;

    void authorizeDeleteSchemaBranch(SecurityContext securityContext,
                                     SchemaMetadataInfo schemaMetadataInfo,
                                     String schemaBranch)
            throws AuthorizationException;

    ///////////////// ConfluentCompatible APIs //////////////////////////////
    Stream<SchemaMetadataInfo> authorizeGetSubjects(SecurityContext securityContext, Stream<SchemaMetadataInfo> stream);

    Stream<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext securityContext, Stream<SchemaVersionInfo> vStream,
                                                      FunctionWithSchemaNotFoundException<Long, SchemaMetadataInfo> getMetadataFunc,
                                                      FunctionWithBranchSchemaNotFoundException<Long, Collection<SchemaBranch>> getBranches);

    @FunctionalInterface
    interface SupplierWithSchemaNotFoundException<T> {
        T get() throws SchemaNotFoundException;
    }

    @FunctionalInterface
    interface FunctionWithSchemaNotFoundException<T, R> {
        R apply(T arg) throws SchemaNotFoundException;
    }

    @FunctionalInterface
    interface FunctionWithBranchSchemaNotFoundException<T, R> {
        R apply(T arg) throws SchemaBranchNotFoundException;
    }
}
