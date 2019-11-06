package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DummyAuthorizationAgent implements AuthorizationAgent {

    @Override
    public Collection<SchemaMetadataInfo> findSchemas(SecurityContext sc,
                                                      Supplier<Collection<SchemaMetadataInfo>> func) {
        return func.get();
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFieldsWithAuthorization
            (SecurityContext sc,
             Function<String, SchemaMetadataInfo> getSchemaMetadataFunc,
             FunctionWithSchemaNotFoundException<SchemaVersionKey, SchemaVersionInfo> getVersionInfoFunc,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionKey>> func)
            throws SchemaNotFoundException {

        return func.get();
    }

    @Override
    public void addSchemaInfoWithAuthorization(SecurityContext sc,
                                               SchemaMetadata schemaMetadata) { }

    @Override
    public void updateSchemaInfoWithAuthorization(SecurityContext sc,
                                                  SchemaMetadata schemaMetadata) { }

    @Override
    public SchemaMetadataInfo getSchemaInfoWithAuthorization(SecurityContext sc,
                                                             SchemaMetadataInfo schemaMetadataInfo) {
        return schemaMetadataInfo;
    }

    @Override
    public void deleteSchemaMetadataWithAuthorization(SecurityContext sc,
                                                      SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public Collection<SchemaVersionInfo> getAllSchemaVersionsWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
            throws SchemaNotFoundException {
        return func.get();
    }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> listAggregatedSchemas
            (SecurityContext sc,
             SupplierWithSchemaNotFoundException<Collection<AggregatedSchemaMetadataInfo>> func)
            throws SchemaNotFoundException {
        return func.get();
    }

    @Override
    public AggregatedSchemaMetadataInfo getAggregatedSchemaInfo(SecurityContext sc,
                                                                AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo) {
        return aggregatedSchemaMetadataInfo;
    }

    @Override
    public List<AggregatedSchemaMetadataInfo> findAggregatedSchemas(SecurityContext sc,
                                                                    List<AggregatedSchemaMetadataInfo> asmi) {
        return asmi;
    }

    @Override
    public void addSchemaVersion(SecurityContext securityContext,
                                 SchemaMetadataInfo schemaMetadataInfo,
                                 String schemaBranch) { }

    @Override
    public void getLatestSchemaVersion(SecurityContext securityContext,
                                       SchemaMetadataInfo schemaMetadataInfo,
                                       String schemaBranch) { }

    @Override
    public void authorizeGetSchemaVersion(SecurityContext securityContext,
                                          SchemaMetadataInfo schemaMetadataInfo,
                                          Collection<SchemaBranch> branches) { }

    @Override
    public void authorizeVerisonStateOperation(SecurityContext securityContext,
                                               SchemaMetadataInfo schemaMetadataInfo,
                                               Collection<SchemaBranch> branches) { }

    @Override
    public void checkCompatibilityWithSchema(SecurityContext securityContext,
                                             SchemaMetadataInfo schemaMetadataInfo,
                                             String schemaBranch) { }

    @Override
    public void getSerializers(SecurityContext securityContext,
                               SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public void uploadFile(SecurityContext securityContext) { }

    @Override
    public void downloadFile(SecurityContext securityContext) { }

    @Override
    public void addSerDes(SecurityContext securityContext) { }

    @Override
    public void getSerDes(SecurityContext securityContext) { }

    @Override
    public void mapSchemaWithSerDes(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public void deleteSchemaVersion(SecurityContext securityContext,
                                    SchemaMetadataInfo schemaMetadataInfo,
                                    Collection<SchemaBranch> branches) { }

    @Override
    public Collection<SchemaBranch> getAllBranches(SecurityContext securityContext,
                                                   SchemaMetadataInfo schemaMetadataInfo,
                                                   SupplierWithSchemaNotFoundException<Collection<SchemaBranch>> func) throws SchemaNotFoundException {
        return func.get();
    }

    @Override
    public void createSchemaBranch(SecurityContext securityContext,
                                   SchemaMetadataInfo schemaMetadataInfo,
                                   Collection<SchemaBranch> branches,
                                   String branchTocreate) { }

    @Override
    public void mergeSchemaVersion(SecurityContext securityContext,
                                   SchemaMetadataInfo schemaMetadataInfo,
                                   Collection<SchemaBranch> schemaBranches) { }

    @Override
    public void deleteSchemaBranch(SecurityContext securityContext,
                                   SchemaMetadataInfo schemaMetadataInfo,
                                   String schemaBranch) { }

    @Override
    public Stream<SchemaMetadataInfo> getSubjects(SecurityContext securityContext, Stream<SchemaMetadataInfo> stream) {
        return stream;
    }

    @Override
    public Stream<SchemaVersionInfo> getAllVersions(SecurityContext securityContext, Stream<SchemaVersionInfo> vStream, FunctionWithSchemaNotFoundException<Long, SchemaMetadataInfo> getMetadataFunc, FunctionWithBranchSchemaNotFoundException<Long, Collection<SchemaBranch>> getBranches) {
        return vStream;
    }
}
