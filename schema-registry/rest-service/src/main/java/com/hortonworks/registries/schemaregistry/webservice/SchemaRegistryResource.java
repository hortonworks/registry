/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.webservice;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
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
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Path("/api/v1/schemaregistry")
@Api(value = "/api/v1/schemaregistry", tags = "schemaregistry", description = "This service is about schema registry operations")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryResource {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryResource.class);

    private final ISchemaRegistry schemaRegistry;

    public SchemaRegistryResource(ISchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @GET
    @Path("/schemas")
    @ApiOperation(value = "List of schemas registered by filtering with the given query parameters",
            response = SchemaMetadata.class, responseContainer = "Collection")
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

            return WSUtils.respond(schemaMetadatas, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schemas", ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/search/fields")
    @ApiOperation(value = "List of schemas containing the given fields",
            response = SchemaVersionKey.class, responseContainer = "Collection")
    @Timed
    public Response findSchemasByFields(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        try {
            Collection<SchemaVersionKey> schemaVersionKeys = schemaRegistry.findSchemasWithFields(buildSchemaFieldQuery(queryParameters));

            return WSUtils.respond(schemaVersionKeys, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
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
    @ApiOperation(value = "Registers the given schema information if it does not exists and returns true if it succeeds",
            notes = "Registers the given schema information and multiple versions of the schema can be registered for the given SchemaInfo's SchemaKey",
            response = Boolean.class)
    @Timed
    public Response addSchemaInfo(@ApiParam(value = "SchemaInfo to be added to the registry", required = true) SchemaMetadata schemaMetadataInfo) {
        Response response;
        try {
            schemaRegistry.addSchemaMetadata(schemaMetadataInfo);
            response = WSUtils.respond(true, Response.Status.CREATED, CatalogResponse.ResponseMessage.SUCCESS);
        }  catch (UnsupportedSchemaTypeException ex) {
            LOG.error("Unsupported schema type encountered while adding schema metadata [{}]",schemaMetadataInfo, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE, ex.getMessage());
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema info [{}] ", schemaMetadataInfo, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}")
    @ApiOperation(value = "Returns schema information with (type, schemaGroup, name)",
            response = SchemaMetadataInfo.class)
    @Timed
    public Response getSchemaInfo(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaName) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadata(schemaName);
            if(schemaMetadataInfo != null) {
                response = WSUtils.respond(schemaMetadataInfo, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo for SchemaKey: [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/{name}/versions")
    @ApiOperation(value = "Registers the given schema version to schema with name and returns respective version number",
            notes = "Registers the given schema version to schema with name if the given schemaText is not registered as a version for this schema, " +
                    "and returns respective version number",
            response = Integer.class)
    @Timed
    public Response addSchema(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaName,
                              @ApiParam(value = "Details about the schema", required = true) SchemaVersion schemaVersion) {

        Response response;
        try {
            LOG.info("schemaVersion for [{}] is [{}]", schemaName, schemaVersion);
            Integer version = schemaRegistry.addSchemaVersion(schemaName, schemaVersion.getSchemaText(), schemaVersion.getDescription());
            response = WSUtils.respond(version, Response.Status.CREATED, CatalogResponse.ResponseMessage.SUCCESS);
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
            LOG.error("Encountered error encountered while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}/versions/latest")
    @ApiOperation(value = "Returns the latest version of a schema with (type, schemaGroup, name)",
            response = SchemaVersionInfo.class)
    @Timed
    public Response getLatestSchemaVersion(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaName) {

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getLatestSchemaVersionInfo(schemaName);
            if (schemaVersionInfo != null) {
                response = WSUtils.respond(schemaVersionInfo, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
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
    @ApiOperation(value = "Returns all version of a schema with the give unique name)",
            response = SchemaVersionInfo.class, responseContainer = "Collection")
    @Timed
    public Response getAllSchemaVersions(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaName) {

        Response response;
        try {
            Collection<SchemaVersionInfo> schemaVersionInfos = schemaRegistry.findAllVersions(schemaName);
            if (schemaVersionInfos != null) {
                response = WSUtils.respond(schemaVersionInfos, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
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
    @ApiOperation(value = "Returns the given version of a schema with the given unque name",
            response = SchemaVersionInfo.class)
    @Timed
    public Response getSchemaVersion(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaMetadata,
                                     @ApiParam(value = "version of the schema", required = true) @PathParam("version") Integer version) {
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadata, version);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
            response = WSUtils.respond(schemaVersionInfo, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
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
    @ApiOperation(value = "Checks whether the given schema text is compatible with all versions of the schema with unique name",
            response = Boolean.class)
    @Timed
    public Response isCompatibleWithSchema(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaName,
                                           @ApiParam(value = "schema text", required = true) String schemaText) {
        Response response;
        try {
            boolean compatible = schemaRegistry.isCompatible(schemaName, schemaText);
            response = WSUtils.respond(compatible, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
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
    @ApiOperation(value = "Returns Serializers registered for schema with (type, schemaGroup, name)",
            response = SerDesInfo.class, responseContainer = "Collection")
    @Timed
    public Response getSerializers(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaName) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfoStorable = schemaRegistry.getSchemaMetadata(schemaName);
            if (schemaMetadataInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaSerializers(schemaMetadataInfoStorable.getId());
                response = WSUtils.respond(schemaSerializers, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
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
    @ApiOperation(value = "Returns Deserializers registered for schema with (type, schemaGroup, name)",
            response = SerDesInfo.class, responseContainer = "Collection")
    @Timed
    public Response getDeserializers(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaMetadata) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfoStorable = schemaRegistry.getSchemaMetadata(schemaMetadata);
            if (schemaMetadataInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaDeserializers(schemaMetadataInfoStorable.getId());
                response = WSUtils.respond(schemaSerializers, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
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
    @ApiOperation(value = "Upload the given file and returns respective identifier.", response = String.class)
    @Timed
    public Response uploadFile(@FormDataParam("file") final InputStream inputStream,
                               @FormDataParam("file") final FormDataContentDisposition contentDispositionHeader) {
        Response response;
        try {
            LOG.info("Received contentDispositionHeader: [{}]", contentDispositionHeader);
            String uploadedFileId = schemaRegistry.uploadFile(inputStream);
            response = WSUtils.respond(uploadedFileId, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while uploading file", ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Produces({"application/octet-stream", "application/json"})
    @Path("/files/download/{fileId}")
    @ApiOperation(value = "Downloads the respective for the given fileId if it exists", response = StreamingOutput.class)
    @Timed
    public Response downloadFile(@ApiParam(value = "Identifier of the file to be downloaded", required = true) @PathParam("fileId") String fileId) {
        Response response;
        try {
            StreamingOutput streamOutput = WSUtils.wrapWithStreamingOutput(schemaRegistry.downloadFile(fileId));
            response = Response.ok(streamOutput).build();
            return response;
        } catch (FileNotFoundException e) {
            LOG.error("No file found for fileId [{}]", fileId, e);
            response = WSUtils.respond(fileId, Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND);
        } catch (Exception ex) {
            LOG.error("Encountered error while downloading file [{}]", fileId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/serializers")
    @ApiOperation(value = "Adds given serialized into registry and returns respective identifier", response = Long.class)
    @Timed
    public Response addSerializer(@ApiParam(value = "Serializer information to be registered", required = true) SerDesInfo serDesInfo) {
        return _addSerDesInfo(serDesInfo);
    }

    @GET
    @Path("/serializers/{id}")
    @ApiOperation(value = "Returns Serializer for the given serializer id.")
    @Timed
    public Response getSerializer(@ApiParam(value = "serializer identifier", required = true) @PathParam("id") Long serializerId) {
        return _getSerDesInfo(serializerId);
    }

    @POST
    @Path("/deserializers")
    @ApiOperation(value = "Adds given deserializer into registry and returns respective identifier", response = Long.class)
    @Timed
    public Response addDeserializer(@ApiParam(value = "Deserializer information to be registered", required = true) SerDesInfo serDesInfo) {
        return _addSerDesInfo(serDesInfo);
    }

    private Response _addSerDesInfo(@ApiParam(value = "Deserializer information to be registered", required = true) SerDesInfo serDesInfo) {
        Response response;
        try {
            Long serializerId = schemaRegistry.addSerDesInfo(serDesInfo);
            response = WSUtils.respond(serializerId, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while adding serializer/deserializer  [{}]", serDesInfo, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/deserializers/{id}")
    @ApiOperation(value = "Returns Serializer for the given serializer id.")
    @Timed
    public Response getDeserializer(@ApiParam(value = "deserializer identifier", required = true) @PathParam("id") Long deserializerId) {
        return _getSerDesInfo(deserializerId);
    }

    private Response _getSerDesInfo(Long serializerId) {
        Response response;
        try {
            SerDesInfo serializerInfo = schemaRegistry.getSerDesInfo(serializerId);
            response = WSUtils.respond(serializerInfo, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while getting serializer/deserializer [{}]", serializerId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @POST
    @Path("/schemas/{name}/mapping/{serDesId}")
    @ApiOperation(value = "Maps the given serializer/deserializer id to the schema represented with (type, schemaGroup, name)")
    @Timed
    public Response mapSerDes(@ApiParam(value = "Name of the schema", required = true) @PathParam("name") String schemaName,
                              @ApiParam(value = "identifier of serializer/deserializer", required = true) @PathParam("serDesId") Long serDesId) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfoStorable = schemaRegistry.getSchemaMetadata(schemaName);
            schemaRegistry.mapSerDesWithSchema(schemaMetadataInfoStorable.getId(), serDesId);
            response = WSUtils.respond(true, Response.Status.OK, CatalogResponse.ResponseMessage.SUCCESS);
        } catch (Exception ex) {
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

}
