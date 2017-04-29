/**
 * Copyright 2016 Hortonworks.
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
 **/
package com.hortonworks.registries.schemaregistry.webservice;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.ha.LeadershipParticipant;
import com.hortonworks.registries.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Schema Registry resource that provides schema registry REST service.
 */
@Path("/v1/schemaregistry")
@Api(value = "/api/v1/schemaregistry", description = "Endpoint for Schema Registry service")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryResource {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryResource.class);

    private final ISchemaRegistry schemaRegistry;
    private final AtomicReference<LeadershipParticipant> leadershipParticipant;

    public SchemaRegistryResource(ISchemaRegistry schemaRegistry, AtomicReference<LeadershipParticipant> leadershipParticipant) {
        Preconditions.checkNotNull(schemaRegistry, "SchemaRegistry can not be null");
        Preconditions.checkNotNull(leadershipParticipant, "LeadershipParticipant can not be null");

        this.schemaRegistry = schemaRegistry;
        this.leadershipParticipant = leadershipParticipant;
    }

    // Hack: Adding number in front of sections to get the ordering in generated swagger documentation correct
    private static final String OPERATION_GROUP_SCHEMA = "1. Schema";
    private static final String OPERATION_GROUP_SERDE = "2. Serializer/Deserializer";
    private static final String OPERATION_GROUP_OTHER = "3. Other";

    @GET
    @Path("/schemaproviders")
    @ApiOperation(value = "Get list of registered Schema Providers",
            notes = "The Schema Registry supports different types of schemas, such as Avro, JSON etc. " + "" +
                    "A Schema Provider is needed for each type of schema supported by the Schema Registry. " +
                    "Schema Provider supports defining schema, serializing and deserializing data using the schema, " +
                    " and checking compatibility between different versions of the schema.",
            response = SchemaProviderInfo.class, responseContainer = "Collection",
            tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response getRegisteredSchemaProviderInfos(@Context UriInfo uriInfo) {
        try {
            Collection<SchemaProviderInfo> schemaProviderInfos = schemaRegistry.getRegisteredSchemaProviderInfos();
            return WSUtils.respondEntities(schemaProviderInfos, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schemas", ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    /**
     * Checks whether the current instance is a leader. If so, it invokes the given {@code supplier}, else current
     * request is redirected to the leader node in registry cluster.
     *
     * @param uriInfo
     * @param supplier
     * @return
     */
    private Response handleLeaderAction(UriInfo uriInfo, Supplier<Response> supplier) {
        LOG.info("URI info [{}]", uriInfo.getRequestUri());
        if (!leadershipParticipant.get().isLeader()) {
            URI location = null;
            try {
                String currentLeaderLoc = leadershipParticipant.get().getCurrentLeader();
                URI leaderServerUrl = new URI(currentLeaderLoc);
                URI requestUri = uriInfo.getRequestUri();
                location = new URI(leaderServerUrl.getScheme(), leaderServerUrl.getAuthority(),
                                   requestUri.getPath(), requestUri.getQuery(), requestUri.getFragment());
                LOG.info("Redirecting to URI [{}] as this instance is not the leader", location);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Response.temporaryRedirect(location).build();
        } else {
            LOG.info("Invoking here as this instance is the leader");
            return supplier.get();
        }
    }

    @GET
    @Path("/schemas")
    @ApiOperation(value = "Get list of schemas by filtering with the given query parameters",
            response = SchemaMetadata.class, responseContainer = "Collection", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response listSchemas(@Context UriInfo uriInfo) {
        try {
            MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
            Map<String, String> filters = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
                List<String> value = entry.getValue();
                filters.put(entry.getKey(), value != null && !value.isEmpty() ? value.get(0) : null);
            }

            Collection<SchemaMetadata> schemaMetadatas = schemaRegistry.findSchemaMetadata(filters);

            return WSUtils.respondEntities(schemaMetadatas, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schemas", ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/search/fields")
    @ApiOperation(value = "Search for schemas containing the given field names",
            notes = "Search the schemas for given field names and return a list of schemas that contain the field.",
            response = SchemaVersionKey.class, responseContainer = "Collection", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response findSchemasByFields(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        try {
            Collection<SchemaVersionKey> schemaVersionKeys = schemaRegistry.findSchemasWithFields(buildSchemaFieldQuery(queryParameters));

            return WSUtils.respondEntities(schemaVersionKeys, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while finding schemas for given fields [{}]", queryParameters, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    private SchemaFieldQuery buildSchemaFieldQuery(MultivaluedMap<String, String> queryParameters) {
        SchemaFieldQuery.Builder builder = new SchemaFieldQuery.Builder();
        for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
            List<String> entryValue = entry.getValue();
            String value = entryValue != null && !entryValue.isEmpty() ? entryValue.get(0) : null;
            if (value != null) {
                if (SchemaFieldInfo.FIELD_NAMESPACE.equals(entry.getKey())) {
                    builder.namespace(value);
                } else if (SchemaFieldInfo.NAME.equals(entry.getKey())) {
                    builder.name(value);
                } else if (SchemaFieldInfo.TYPE.equals(entry.getKey())) {
                    builder.type(value);
                }
            }
        }

        return builder.build();
    }

    @POST
    @Path("/schemas")
    @ApiOperation(value = "Create a schema if it does not already exist",
            notes = "Creates a schema with the given schema information if it does not already exist." +
                    " A unique schema identifier is returned.",
            response = Long.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response addSchemaInfo(@ApiParam(value = "Schema to be added to the registry", required = true)
                                          SchemaMetadata schemaMetadataInfo,
                                  @Context UriInfo uriInfo) {
        return handleLeaderAction(uriInfo, () -> {
            Response response;
            try {
                schemaMetadataInfo.trim();
                checkValueAsNullOrEmpty("Schema name", schemaMetadataInfo.getName());
                checkValueAsNullOrEmpty("Schema type", schemaMetadataInfo.getType());
                boolean throwErrorIfExists = isThrowErrorIfExists(uriInfo);
                Long schemaId = schemaRegistry.addSchemaMetadata(schemaMetadataInfo, throwErrorIfExists);
                response = WSUtils.respondEntity(schemaId, Response.Status.CREATED);
            } catch (IllegalArgumentException ex) {
                LOG.error("Expected parameter is invalid", schemaMetadataInfo, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING, ex.getMessage());
            } catch (UnsupportedSchemaTypeException ex) {
                LOG.error("Unsupported schema type encountered while adding schema metadata [{}]", schemaMetadataInfo, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE, ex.getMessage());
            } catch (Exception ex) {
                LOG.error("Error encountered while adding schema info [{}] ", schemaMetadataInfo, ex);
                response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR,
                                           CatalogResponse.ResponseMessage.EXCEPTION,
                                           String.format("Storing the given SchemaMetadata [%s] is failed",schemaMetadataInfo.toString()));
            }

            return response;
        });
    }

    private boolean isThrowErrorIfExists(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        if(queryParameters == null) {
            return false;
        }

        List<String> values = queryParameters.get("_throwErrorIfExists");
        return values != null && !values.isEmpty() && Boolean.getBoolean(values.get(0));
    }

    @GET
    @Path("/schemas/{name}")
    @ApiOperation(value = "Get schema information for the given schema name",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getSchemaInfo(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadata(schemaName);
            if (schemaMetadataInfo != null) {
                response = WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo with name: [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemasById/{schemaId}")
    @ApiOperation(value = "Get schema for a given schema identifier",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getSchemaInfo(@ApiParam(value = "Schema identifier", required = true) @PathParam("schemaId") Long schemaId) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadata(schemaId);
            if (schemaMetadataInfo != null) {
                response = WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaId.toString());
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo with schemaId: [{}]", schemaId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/{name}/versions/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(value = "Register a new version of the schema by uploading schema version text",
            notes = "Registers the given schema version to schema with name if the given file content is not registered as a version for this schema, " +
                    "and returns respective version number." +
                    "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
            response = Integer.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response uploadSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name")
                                                String schemaName,
                                        @ApiParam(value = "Schema version text file to be uploaded", required = true)
                                        @FormDataParam("file") final InputStream inputStream,
                                        @ApiParam(value = "Description about the schema version to be uploaded", required = true)
                                        @FormDataParam("description") final String description,
                                        @Context UriInfo uriInfo) {
        return handleLeaderAction(uriInfo, () -> {
            Response response;
            SchemaVersion schemaVersion = null;
            try {
                schemaVersion = new SchemaVersion(IOUtils.toString(inputStream, "UTF-8"),
                                                  description);
                response = addSchema(schemaName, schemaVersion, uriInfo);
            } catch (IOException ex) {
                LOG.error("Encountered error while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex, ex);
                response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
            }

            return response;
        });
    }

    @POST
    @Path("/schemas/{name}/versions")
    @ApiOperation(value = "Register a new version of the schema",
            notes = "Registers the given schema version to schema with name if the given schemaText is not registered as a version for this schema, " +
                    "and returns respective version number." +
                    "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
            response = Integer.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response addSchema(@ApiParam(value = "Schema name", required = true) @PathParam("name")
                                      String schemaName,
                              @ApiParam(value = "Details about the schema", required = true)
                                      SchemaVersion schemaVersion,
                              @Context UriInfo uriInfo) {
        return handleLeaderAction(uriInfo, () -> {
            Response response;
            try {
                LOG.info("schemaVersion for [{}] is [{}]", schemaName, schemaVersion);
                Integer version = schemaRegistry.addSchemaVersion(schemaName, schemaVersion.getSchemaText(), schemaVersion.getDescription());
                response = WSUtils.respondEntity(version, Response.Status.CREATED);
            } catch (InvalidSchemaException ex) {
                LOG.error("Invalid schema error encountered while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INVALID_SCHEMA, ex.getMessage());
            } catch (IncompatibleSchemaException ex) {
                LOG.error("Incompatible schema error encountered while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA, ex.getMessage());
            } catch (UnsupportedSchemaTypeException ex) {
                LOG.error("Unsupported schema type encountered while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE, ex.getMessage());
            } catch (Exception ex) {
                LOG.error("Encountered error while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex, ex);
                response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
            }

            return response;
        });
    }

    @GET
    @Path("/schemas/{name}/versions/latest")
    @ApiOperation(value = "Get the latest version of the schema for the given schema name",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getLatestSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName) {

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getLatestSchemaVersionInfo(schemaName);
            if (schemaVersionInfo != null) {
                response = WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting latest schema version for schemakey [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;

    }

    @GET
    @Path("/schemas/{name}/versions")
    @ApiOperation(value = "Get all the versions of the schema for the given schema name)",
            response = SchemaVersionInfo.class, responseContainer = "Collection", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getAllSchemaVersions(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName) {

        Response response;
        try {
            Collection<SchemaVersionInfo> schemaVersionInfos = schemaRegistry.findAllVersions(schemaName);
            if (schemaVersionInfos != null) {
                response = WSUtils.respondEntities(schemaVersionInfos, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting all schema versions for schemakey [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}/versions/{version}")
    @ApiOperation(value = "Get a version of the schema identified by the schema name",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaMetadata,
                                     @ApiParam(value = "version of the schema", required = true) @PathParam("version") Integer version) {
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadata, version);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
            response = WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schemas found with schemaVersionKey: [{}]", schemaVersionKey);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaVersionKey.toString());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting all schema versions for schemakey [{}]", schemaMetadata, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/{name}/compatibility")
    @ApiOperation(value = "Checks if the given schema text is compatible with all the versions of the schema identified by the name",
            response = CompatibilityResult.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response checkCompatibilityWithSchema(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                                 @ApiParam(value = "schema text", required = true) String schemaText) {
        Response response;
        try {
            CompatibilityResult compatible = schemaRegistry.checkCompatibility(schemaName, schemaText);
            response = WSUtils.respondEntity(compatible, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.error("No schemas found with schemakey: [{}]", schemaName, e);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
        } catch (Exception ex) {
            LOG.error("Encountered error while checking compatibility with versions of schema with [{}] for given schema text [{}]", schemaName, schemaText, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}/serializers")
    @ApiOperation(value = "Get list of Serializers registered for the given schema name",
            response = SerDesInfo.class, responseContainer = "Collection", tags = OPERATION_GROUP_SERDE)
    @Timed
    public Response getSerializers(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfoStorable = schemaRegistry.getSchemaMetadata(schemaName);
            if (schemaMetadataInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaSerializers(schemaMetadataInfoStorable.getId());
                response = WSUtils.respondEntities(schemaSerializers, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting serializers for schemaKey [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}/deserializers")
    @ApiOperation(value = "Get list of Deserializers registered for the given schema name",
            response = SerDesInfo.class, responseContainer = "Collection", tags = OPERATION_GROUP_SERDE)
    @Timed
    public Response getDeserializers(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaMetadata) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfoStorable = schemaRegistry.getSchemaMetadata(schemaMetadata);
            if (schemaMetadataInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaDeserializers(schemaMetadataInfoStorable.getId());
                response = WSUtils.respondEntities(schemaSerializers, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaMetadata);
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaMetadata);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting deserializers for schemaKey [{}]", schemaMetadata, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/files")
    @ApiOperation(value = "Upload the given file and returns respective identifier.", response = String.class, tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response uploadFile(@FormDataParam("file") final InputStream inputStream,
                               @FormDataParam("file") final FormDataContentDisposition contentDispositionHeader) {
        Response response;
        try {
            LOG.info("Received contentDispositionHeader: [{}]", contentDispositionHeader);
            String uploadedFileId = schemaRegistry.uploadFile(inputStream);
            response = WSUtils.respondEntity(uploadedFileId, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while uploading file", ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Produces({"application/octet-stream", "application/json"})
    @Path("/files/download/{fileId}")
    @ApiOperation(value = "Downloads the respective for the given fileId if it exists", response = StreamingOutput.class, tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response downloadFile(@ApiParam(value = "Identifier of the file to be downloaded", required = true) @PathParam("fileId") String fileId) {
        Response response;
        try {
            StreamingOutput streamOutput = WSUtils.wrapWithStreamingOutput(schemaRegistry.downloadFile(fileId));
            response = Response.ok(streamOutput).build();
            return response;
        } catch (FileNotFoundException e) {
            LOG.error("No file found for fileId [{}]", fileId, e);
            response = WSUtils.respondEntity(fileId, Response.Status.NOT_FOUND);
        } catch (Exception ex) {
            LOG.error("Encountered error while downloading file [{}]", fileId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/serializers")
    @ApiOperation(value = "Add a Serializer into the Schema Registry", response = Long.class, tags = OPERATION_GROUP_SERDE)
    @Timed
    public Response addSerializer(@ApiParam(value = "Serializer information to be registered", required = true) SerDesInfo serDesInfo,
                                  @Context UriInfo uriInfo) {
        return handleLeaderAction(uriInfo, () -> {
            return _addSerDesInfo(serDesInfo);
        });
    }

    @GET
    @Path("/serializers/{id}")
    @ApiOperation(value = "Get a Serializer for the given serializer id", tags = OPERATION_GROUP_SERDE)
    @Timed
    public Response getSerializer(@ApiParam(value = "Serializer identifier", required = true) @PathParam("id") Long serializerId) {
        return _getSerDesInfo(serializerId);
    }

    @POST
    @Path("/deserializers")
    @ApiOperation(value = "Add a Deserializer into Schema Registry", response = Long.class, tags = OPERATION_GROUP_SERDE)
    @Timed
    public Response addDeserializer(@ApiParam(value = "Deserializer information to be registered", required = true) SerDesInfo serDesInfo,
                                    @Context UriInfo uriInfo) {
        return handleLeaderAction(uriInfo, () -> {
            return _addSerDesInfo(serDesInfo);
        });
    }

    private Response _addSerDesInfo(@ApiParam(value = "Deserializer information to be registered", required = true) SerDesInfo serDesInfo) {
        Response response;
        try {
            Long serializerId = schemaRegistry.addSerDesInfo(serDesInfo);
            response = WSUtils.respondEntity(serializerId, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while adding serializer/deserializer  [{}]", serDesInfo, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/deserializers/{id}")
    @ApiOperation(value = "Get a Deserializer for the given deserializer id", tags = OPERATION_GROUP_SERDE)
    @Timed
    public Response getDeserializer(@ApiParam(value = "Deserializer identifier", required = true) @PathParam("id") Long deserializerId) {
        return _getSerDesInfo(deserializerId);
    }

    private Response _getSerDesInfo(Long serializerId) {
        Response response;
        try {
            SerDesInfo serializerInfo = schemaRegistry.getSerDesInfo(serializerId);
            response = WSUtils.respondEntity(serializerInfo, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while getting serializer/deserializer [{}]", serializerId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @POST
    @Path("/schemas/{name}/mapping/{serDesId}")
    @ApiOperation(value = "Bind the given Serializer/Deserializer to the schema identified by the schema name", tags = OPERATION_GROUP_SERDE)
    @Timed
    public Response mapSerDes(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                              @ApiParam(value = "Serializer/deserializer identifier", required = true) @PathParam("serDesId") Long serDesId,
                              @Context UriInfo uriInfo) {
        return handleLeaderAction(uriInfo, () -> {
            Response response;
            try {
                SchemaMetadataInfo schemaMetadataInfoStorable = schemaRegistry.getSchemaMetadata(schemaName);
                schemaRegistry.mapSerDesWithSchema(schemaMetadataInfoStorable.getId(), serDesId);
                response = WSUtils.respondEntity(true, Response.Status.OK);
            } catch (Exception ex) {
                response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
            }

            return response;
        });
    }

    private static void checkValueAsNullOrEmpty(String name, String value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Parameter " + name + " is null");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Parameter " + name + " is empty");
        }
    }
}
