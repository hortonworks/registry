package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.hadoop.security.authorize.AuthorizationException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface AuthorizationAgent {

    Collection<AggregatedSchemaMetadataInfo> authorizeListAggregatedSchemas
            (SecurityContext sc,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList);

    AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(SecurityContext sc,
                                                                  AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo)
            throws AuthorizationException;

    Collection<SchemaMetadataInfo> authorizeFindSchemas(SecurityContext sc,
                                                        Supplier<Collection<SchemaMetadataInfo>> func);

    List<AggregatedSchemaMetadataInfo> authorizeFindAggregatedSchemas(SecurityContext sc,
                                                                      List<AggregatedSchemaMetadataInfo> asmi);

    Collection<SchemaVersionKey> authorizeFindSchemasByFields
            (SecurityContext sc,
             Function<String, SchemaMetadataInfo> getSchemaMetadataFunc,
             FunctionWithSchemaNotFoundException<SchemaVersionKey, SchemaVersionInfo> getVersionInfoFunc,
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


    void addSchemaVersion(SecurityContext securityContext,
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
