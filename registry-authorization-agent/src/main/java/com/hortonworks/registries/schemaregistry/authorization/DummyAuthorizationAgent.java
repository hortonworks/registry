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
    public Collection<SchemaMetadataInfo> authorizeFindSchemas(SecurityContext sc,
                                                               Collection<SchemaMetadataInfo> schemas) {
        return schemas;
    }

    @Override
    public Collection<SchemaVersionKey> authorizeFindSchemasByFields
            (SecurityContext sc,
             Function<String, SchemaMetadataInfo> getSchemaMetadataFunc,
             FunctionWithSchemaNotFoundException<SchemaVersionKey, SchemaVersionInfo> getVersionInfoFunc,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionKey>> func)
            throws SchemaNotFoundException {

        return func.get();
    }

    @Override
    public void authorizeAddSchemaInfo(SecurityContext sc,
                                       SchemaMetadata schemaMetadata) { }

    @Override
    public void authorizeUpdateSchemaInfo(SecurityContext sc,
                                          SchemaMetadata schemaMetadata) { }

    @Override
    public SchemaMetadataInfo authorizeGetSchemaInfo(SecurityContext sc,
                                                     SchemaMetadataInfo schemaMetadataInfo) {
        return schemaMetadataInfo;
    }

    @Override
    public void authorizeDeleteSchemaMetadata(SecurityContext sc,
                                              SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public Collection<SchemaVersionInfo> authorizeGetAllSchemaVersions
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
            throws SchemaNotFoundException {
        return func.get();
    }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> authorizeListAggregatedSchemas
            (SecurityContext sc,
             Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfoList) {
        return aggregatedSchemaMetadataInfoList;
    }

    @Override
    public AggregatedSchemaMetadataInfo authorizeGetAggregatedSchemaInfo(SecurityContext sc,
                                                                         AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo) {
        return aggregatedSchemaMetadataInfo;
    }

    @Override
    public List<AggregatedSchemaMetadataInfo> authorizeFindAggregatedSchemas(SecurityContext sc,
                                                                             List<AggregatedSchemaMetadataInfo> asmi) {
        return asmi;
    }

    @Override
    public void addSchemaVersion(SecurityContext securityContext,
                                 SchemaMetadataInfo schemaMetadataInfo,
                                 String schemaBranch) { }

    @Override
    public void authorizeGetLatestSchemaVersion(SecurityContext securityContext,
                                                SchemaMetadataInfo schemaMetadataInfo,
                                                String schemaBranch) { }

    @Override
    public void authorizeGetSchemaVersion(SecurityContext securityContext,
                                          SchemaMetadataInfo schemaMetadataInfo,
                                          Collection<SchemaBranch> branches) { }

    @Override
    public void authorizeVersionStateOperation(SecurityContext securityContext,
                                               SchemaMetadataInfo schemaMetadataInfo,
                                               Collection<SchemaBranch> branches) { }

    @Override
    public void authorizeCheckCompatibilityWithSchema(SecurityContext securityContext,
                                                      SchemaMetadataInfo schemaMetadataInfo,
                                                      String schemaBranch) { }

    @Override
    public void authorizeGetSerializers(SecurityContext securityContext,
                                        SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public void authorizeUploadFile(SecurityContext securityContext) { }

    @Override
    public void authorizeDownloadFile(SecurityContext securityContext) { }

    @Override
    public void authorizeAddSerDes(SecurityContext securityContext) { }

    @Override
    public void authorizeGetSerDes(SecurityContext securityContext) { }

    @Override
    public void authorizeMapSchemaWithSerDes(SecurityContext securityContext, SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public void authorizeDeleteSchemaVersion(SecurityContext securityContext,
                                             SchemaMetadataInfo schemaMetadataInfo,
                                             Collection<SchemaBranch> branches) { }

    @Override
    public Collection<SchemaBranch> authorizeGetAllBranches(SecurityContext securityContext,
                                                            SchemaMetadataInfo schemaMetadataInfo,
                                                            SupplierWithSchemaNotFoundException<Collection<SchemaBranch>> func) throws SchemaNotFoundException {
        return func.get();
    }

    @Override
    public void authorizeCreateSchemaBranch(SecurityContext securityContext,
                                            SchemaMetadataInfo schemaMetadataInfo,
                                            Collection<SchemaBranch> branches,
                                            String branchTocreate) { }

    @Override
    public void authorizeMergeSchemaVersion(SecurityContext securityContext,
                                            SchemaMetadataInfo schemaMetadataInfo,
                                            Collection<SchemaBranch> schemaBranches) { }

    @Override
    public void authorizeDeleteSchemaBranch(SecurityContext securityContext,
                                            SchemaMetadataInfo schemaMetadataInfo,
                                            String schemaBranch) { }

    @Override
    public Stream<SchemaMetadataInfo> authorizeGetSubjects(SecurityContext securityContext, Stream<SchemaMetadataInfo> stream) {
        return stream;
    }

    @Override
    public Stream<SchemaVersionInfo> authorizeGetAllVersions(SecurityContext securityContext, Stream<SchemaVersionInfo> vStream, FunctionWithSchemaNotFoundException<Long, SchemaMetadataInfo> getMetadataFunc, FunctionWithBranchSchemaNotFoundException<Long, Collection<SchemaBranch>> getBranches) {
        return vStream;
    }
}
