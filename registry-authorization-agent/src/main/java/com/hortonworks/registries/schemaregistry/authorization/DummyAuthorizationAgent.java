package com.hortonworks.registries.schemaregistry.authorization;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.hadoop.security.authorize.AuthorizationException;

import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

public class DummyAuthorizationAgent implements AuthorizationAgent {

    @Override
    public Collection<SchemaMetadataInfo> findSchemasWithAuthorization(SecurityContext sc,
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
                                               SchemaMetadata schemaMetadata)
            throws AuthorizationException { }

    @Override
    public void updateSchemaInfoWithAuthorization(SecurityContext sc,
                                                  SchemaMetadata schemaMetadata)
            throws AuthorizationException { }

    @Override
    public SchemaMetadataInfo getSchemaInfoWithAuthorization(SecurityContext sc,
                                                             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException {

        return schemaMetadataInfo;
    }

    @Override
    public void deleteSchemaMetadataWithAuthorization(SecurityContext sc,
                                                      SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException { }

    @Override
    public Collection<SchemaVersionInfo> getAllSchemaVersionsWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
            throws SchemaNotFoundException, AuthorizationException {
        return func.get();
    }

    @Override
    public SchemaVersionInfo getSchemaVersionWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             Integer schemaVersion,
             SupplierWithSchemaNotFoundException<SchemaVersionInfo> func)
            throws SchemaNotFoundException, AuthorizationException {

        return func.get();
    }
}
