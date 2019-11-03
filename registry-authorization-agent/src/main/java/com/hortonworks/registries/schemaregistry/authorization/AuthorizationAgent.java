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

public interface AuthorizationAgent {

    Collection<SchemaMetadataInfo> findSchemasWithAuthorization(SecurityContext sc,
                                                                Supplier<Collection<SchemaMetadataInfo>> func);

    Collection<SchemaVersionKey> findSchemasByFieldsWithAuthorization
            (SecurityContext sc,
             Function<String, SchemaMetadataInfo> getSchemaMetadataFunc,
             FunctionWithSchemaNotFoundException<SchemaVersionKey, SchemaVersionInfo> getVersionInfoFunc,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionKey>> func)
            throws SchemaNotFoundException;

    void  addSchemaInfoWithAuthorization(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException;

    void updateSchemaInfoWithAuthorization(SecurityContext sc, SchemaMetadata schemaMetadata)
            throws AuthorizationException;

    SchemaMetadataInfo getSchemaInfoWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws AuthorizationException;

    void deleteSchemaMetadataWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo)
            throws  AuthorizationException;

    Collection<SchemaVersionInfo> getAllSchemaVersionsWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             SupplierWithSchemaNotFoundException<Collection<SchemaVersionInfo>> func)
            throws SchemaNotFoundException, AuthorizationException;

    SchemaVersionInfo getSchemaVersionWithAuthorization
            (SecurityContext sc,
             SchemaMetadataInfo schemaMetadataInfo,
             String schemaBranchName,
             Integer schemaVersion,
             SupplierWithSchemaNotFoundException<SchemaVersionInfo> func)
            throws SchemaNotFoundException, AuthorizationException;


    @FunctionalInterface
    interface SupplierWithSchemaNotFoundException<T> {
        T get() throws SchemaNotFoundException;
    }

    @FunctionalInterface
    interface FunctionWithSchemaNotFoundException<T, R> {
        R apply(T arg) throws SchemaNotFoundException;
    }
}
