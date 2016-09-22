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
import com.hortonworks.iotas.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfoStorable;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaInstanceDetails;
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

import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.EXCEPTION;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.INVALID_SCHEMA;
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.SUCCESS;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

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
            response = SchemaVersionKey.class, responseContainer = "Collection")
    @Timed
    public Response listSchemas(@Context UriInfo uriInfo) {
        try {
            MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
            Map<String, String> filters = new HashMap<>();
            queryParameters.forEach((key, values) -> filters.put(key, values != null && !values.isEmpty() ? values.get(0) : null));
            Collection<SchemaVersionKey> schemaVersionKeys = schemaRegistry.findSchemas(filters);

            return WSUtils.respond(schemaVersionKeys, OK, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schemas", ex);
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
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

            return WSUtils.respond(schemaVersionKeys, OK, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while finding schemas for given fields [{}]", queryParameters, ex);
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    private SchemaFieldQuery buildSchemaFieldQuery(MultivaluedMap<String, String> queryParameters) {
        SchemaFieldQuery.Builder builder = new SchemaFieldQuery.Builder();
        for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
            List<String> entryValue = entry.getValue();
            String value = entryValue != null && !entryValue.isEmpty() ? entryValue.get(0) : null;
            if (value != null) {
                if (SchemaFieldInfoStorable.FIELD_NAMESPACE.equals(entry.getKey())) {
                    builder.namespace(value);
                } else if (SchemaFieldInfoStorable.NAME.equals(entry.getKey())) {
                    builder.name(value);
                } else if (SchemaFieldInfoStorable.TYPE.equals(entry.getKey())) {
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
    public Response addSchemaInfo(@ApiParam(value = "SchemaInfo needs to be added to the registry", required = true) SchemaInfo schemaInfo) {
        Response response;
        try {
            schemaRegistry.addSchemaInfo(schemaInfo);
            response = WSUtils.respond(true, CREATED, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema info [{}] ", schemaInfo, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}")
    @ApiOperation(value = "Returns schema information with (type, schemaGroup, name)",
            response = SchemaInfo.class)
    @Timed
    public Response getSchemaInfo(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                                  @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                                  @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            SchemaInfo schemaInfo = schemaRegistry.getSchemaInfo(schemaKey);
            response = WSUtils.respond(schemaInfo, OK, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo for SchemaKey: [{}]", schemaKey, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}")
    @ApiOperation(value = "Registers the given schema version to schema with (type, schemaGroup, name) and returns respective version number",
            notes = "Registers the given schema version to schema with (type, schemaGroup, name) if the given schemaText is not registered as a version for this schema, " +
                    "and returns respective version number",
            response = Integer.class)
    @Timed
    public Response addSchema(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                              @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                              @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name,
                              @ApiParam(value = "Details about the schema", required = true) SchemaInstanceDetails schemaInstanceDetails) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        SchemaInfo schemaInfo = new SchemaInfo(schemaKey,
                schemaInstanceDetails.getSchemaDescription(),
                schemaInstanceDetails.getCompatibility());
        Response response;
        try {
            Integer version = schemaRegistry.addSchemaVersion(schemaInfo, schemaInstanceDetails.getSchemaVersion());
            response = WSUtils.respond(version, CREATED, SUCCESS);
        } catch (InvalidSchemaException ex) {
            LOG.error("Invalid schema error encountered while adding schema [{}]", schemaInfo, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, INVALID_SCHEMA, ex.getMessage());
        } catch (IncompatibleSchemaException ex) {
            LOG.error("Incompatible schema error encountered while adding schema [{}]", schemaInfo, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, INCOMPATIBLE_SCHEMA, ex.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while adding schema: [{}]", schemaInfo, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/versions/latest")
    @ApiOperation(value = "Returns the latest version of a schema with (type, schemaGroup, name)",
            response = SchemaVersionInfo.class)
    @Timed
    public Response getLatestSchemaVersion(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                                           @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                                           @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getLatestSchemaVersionInfo(schemaKey);
            if (schemaVersionInfo != null) {
                response = WSUtils.respond(schemaVersionInfo, OK, SUCCESS);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaKey);
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting latest schema version for schemakey [{}]", schemaKey, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;

    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/versions")
    @ApiOperation(value = "Returns all version of a schema with (type, schemaGroup, name)",
            response = SchemaVersionInfo.class, responseContainer = "Collection")
    @Timed
    public Response getAllSchemaVersions(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                                         @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                                         @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);

        Response response;
        try {
            Collection<SchemaVersionInfo> schemaVersionInfos = schemaRegistry.findAllVersions(schemaKey);
            if (schemaVersionInfos != null) {
                response = WSUtils.respond(schemaVersionInfos, OK, SUCCESS);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaKey);
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting all schema versions for schemakey [{}]", schemaKey, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/versions/{version}")
    @ApiOperation(value = "Returns the given version of a schema with (type, schemaGroup, name)",
            response = SchemaVersionInfo.class)
    @Timed
    public Response getSchemaVersion(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                                     @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                                     @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name,
                                     @ApiParam(value = "version of the schema", required = true) @PathParam("version") Integer version) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaKey, version);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
            response = WSUtils.respond(schemaVersionInfo, OK, SUCCESS);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schemas found with schemaVersionKey: [{}]", schemaVersionKey);
            response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaVersionKey.toString());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting all schema versions for schemakey [{}]", schemaKey, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/compatibility")
    @ApiOperation(value = "Checks whether the given schema text is compatible with all versions of the schema with (type, schemaGroup, name)",
            response = Boolean.class)
    @Timed
    public Response isCompatibleWithSchema(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                                           @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                                           @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name,
                                           @ApiParam(value = "schema text", required = true) String schemaText) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            boolean compatible = schemaRegistry.isCompatible(schemaKey, schemaText);
            response = WSUtils.respond(compatible, OK, SUCCESS);
        } catch (SchemaNotFoundException e) {
            LOG.error("No schemas found with schemakey: [{}]", schemaKey, e);
            response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
        } catch (Exception ex) {
            LOG.error("Encountered error while checking compatibility with versions of schema with [{}] for given schema text [{}]", schemaKey, schemaText, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/serializers")
    @ApiOperation(value = "Returns Serializers registered for schema with (type, schemaGroup, name)",
            response = SerDesInfo.class, responseContainer = "Collection")
    @Timed
    public Response getSerializers(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                                   @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                                   @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            SchemaInfo schemaInfoStorable = schemaRegistry.getSchemaInfo(schemaKey);
            if (schemaInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaSerializers(schemaInfoStorable.getId());
                response = WSUtils.respond(schemaSerializers, OK, SUCCESS);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaKey);
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting serializers for schemaKey [{}]", schemaKey, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/deserializers")
    @ApiOperation(value = "Returns Deserializers registered for schema with (type, schemaGroup, name)",
            response = SerDesInfo.class, responseContainer = "Collection")
    @Timed
    public Response getDeserializers(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                                     @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                                     @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            SchemaInfo schemaInfoStorable = schemaRegistry.getSchemaInfo(schemaKey);
            if (schemaInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaDeserializers(schemaInfoStorable.getId());
                response = WSUtils.respond(schemaSerializers, OK, SUCCESS);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaKey);
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting deserializers for schemaKey [{}]", schemaKey, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
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
            response = WSUtils.respond(uploadedFileId, OK, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while uploading file", ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
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
            response = WSUtils.respond(fileId, NOT_FOUND, ENTITY_NOT_FOUND);
        } catch (Exception ex) {
            LOG.error("Encountered error while downloading file [{}]", fileId, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
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
            response = WSUtils.respond(serializerId, OK, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while adding serializer/deserializer  [{}]", serDesInfo, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
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
            response = WSUtils.respond(serializerInfo, OK, SUCCESS);
        } catch (Exception ex) {
            LOG.error("Encountered error while getting serializer/deserializer [{}]", serializerId, ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @POST
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/mapping/{serDesId}")
    @ApiOperation(value = "Maps the given serializer/deserializer id to the schema represented with (type, schemaGroup, name)")
    @Timed
    public Response mapSerDes(@ApiParam(value = "Type of the schema", required = true) @PathParam("type") String type,
                              @ApiParam(value = "Group of the schema", required = true) @PathParam("schemaGroup") String schemaGroup,
                              @ApiParam(value = "Name of the schema", required = true) @PathParam("name") String name,
                              @ApiParam(value = "identifier of serializer/deserializer", required = true) @PathParam("serDesId") Long serDesId) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            SchemaInfo schemaInfoStorable = schemaRegistry.getSchemaInfo(schemaKey);
            schemaRegistry.mapSerDesWithSchema(schemaInfoStorable.getId(), serDesId);
            response = WSUtils.respond(true, OK, SUCCESS);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

}
