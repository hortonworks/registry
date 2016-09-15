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
import com.hortonworks.registries.schemaregistry.SchemaFieldInfoStorable;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaDetails;
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
import static com.hortonworks.iotas.common.catalog.CatalogResponse.ResponseMessage.SUCCESS;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

/**
 *
 */
@Path("/api/v1/schemaregistry")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryCatalog.class);

    private final ISchemaRegistry schemaRegistry;

    public SchemaRegistryCatalog(ISchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @GET
    @Path("/schemas")
    @Timed
    public Response listSchemas(@Context UriInfo uriInfo) {
        try {
            MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
            Map<String, String> filters = new HashMap<>();
            queryParameters.forEach((key, values) -> filters.put(key, values != null && !values.isEmpty() ? values.get(0) : null));
            Collection<SchemaVersionKey> schemaVersionKeys = schemaRegistry.findSchemas(filters);

            return WSUtils.respond(OK, SUCCESS, schemaVersionKeys);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/search/fields")
    @Timed
    public Response findSchemasbyFields(@Context UriInfo uriInfo) {
        try {
            MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
            Collection<SchemaVersionKey> schemaVersionKeys = schemaRegistry.findSchemasWithFields(buildSchemaFieldQuery(queryParameters));

            return WSUtils.respond(OK, SUCCESS, schemaVersionKeys);
        } catch (Exception ex) {
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
    @Timed
    public Response addSchema(SchemaInfo schemaInfo) {
        Response response;
        try {
            schemaRegistry.addSchemaMetadata(schemaInfo);
            response = WSUtils.respond(CREATED, SUCCESS, true);
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema", ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}")
    @Timed
    public Response addSchema(@PathParam("type") String type,
                              @PathParam("schemaGroup") String schemaGroup,
                              @PathParam("name") String name,
                              SchemaDetails schemaDetails) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        SchemaInfo schemaInfo = new SchemaInfo(schemaKey,
                schemaDetails.getSchemaMetadataDescription(),
                schemaDetails.getCompatibility());
        Response response;
        try {
            Integer version = schemaRegistry.addSchema(schemaInfo, schemaDetails.getVersionedSchema());
            response = WSUtils.respond(CREATED, SUCCESS, version);
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema", ex);
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }


    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/versions/latest")
    @Timed
    public Response getSchemaInstance(@PathParam("type") String type,
                                      @PathParam("schemaGroup") String schemaGroup,
                                      @PathParam("name") String name) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getLatestSchemaInfo(schemaKey);
            if (schemaVersionInfo != null) {
                response = WSUtils.respond(OK, SUCCESS, schemaVersionInfo);
            } else {
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;

    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/versions")
    @Timed
    public Response getSchemaInstances(@PathParam("type") String type,
                                       @PathParam("schemaGroup") String schemaGroup,
                                       @PathParam("name") String name) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);

        Response response;
        try {
            Collection<SchemaVersionInfo> schemaVersionInfos = schemaRegistry.findAllVersions(schemaKey);
            if (schemaVersionInfos != null) {
                response = WSUtils.respond(OK, SUCCESS, schemaVersionInfos);
            } else {
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/versions/{version}")
    @Timed
    public Response getSchemaInstance(@PathParam("type") String type,
                                      @PathParam("schemaGroup") String schemaGroup,
                                      @PathParam("name") String name,
                                      @PathParam("version") Integer version) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaKey, version);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaInfo(schemaVersionKey);
            response = WSUtils.respond(OK, SUCCESS, schemaVersionInfo);
        } catch (SchemaNotFoundException e) {
            response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaVersionKey.toString());
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/compatibility")
    @Timed
    public Response isCompatibleWithSchema(@PathParam("type") String type,
                                           @PathParam("schemaGroup") String schemaGroup,
                                           @PathParam("name") String name,
                                           String schema) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            boolean compatible = schemaRegistry.isCompatible(schemaKey, schema);
            response = WSUtils.respond(OK, SUCCESS, compatible);
        } catch (SchemaNotFoundException e) {
            response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/serializers")
    @Timed
    public Response getSerializers(@PathParam("type") String type,
                                   @PathParam("schemaGroup") String schemaGroup,
                                   @PathParam("name") String name,
                                   String schema) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            SchemaInfo schemaInfoStorable = schemaRegistry.getSchemaMetadata(schemaKey);
            if (schemaInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaSerializers(schemaInfoStorable.getId());
                response = WSUtils.respond(OK, SUCCESS, schemaSerializers);
            } else {
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }


    @GET
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/deserializers")
    @Timed
    public Response getDeserializers(@PathParam("type") String type,
                                     @PathParam("schemaGroup") String schemaGroup,
                                     @PathParam("name") String name,
                                     String schema) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            SchemaInfo schemaInfoStorable = schemaRegistry.getSchemaMetadata(schemaKey);
            if (schemaInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSchemaDeserializers(schemaInfoStorable.getId());
                response = WSUtils.respond(OK, SUCCESS, schemaSerializers);
            } else {
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaKey.toString());
            }
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Produces({"application/octet-stream", "application/json"})
    @Path("/schemas/{id}/serializers/{serializerId}")
    @Timed
    public Response downloadSerializer(@PathParam("id") Long schemaMetadataId, @PathParam("serializerId") Long serializerId) {
        Response response;
        try {
            InputStream inputStream = schemaRegistry.downloadJar(serializerId);
            if (inputStream != null) {
                StreamingOutput streamOutput = WSUtils.wrapWithStreamingOutput(inputStream);
                response = Response.ok(streamOutput).build();
            } else {
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
            }
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @Timed
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/files")
    public Response uploadFile(@FormDataParam("file") final InputStream inputStream,
                               @FormDataParam("file") final FormDataContentDisposition contentDispositionHeader) {
        // todo we should be using file upload resources in iotas.
        Response response;
        try {
            LOG.info("Received contentDispositionHeader: [{}]", contentDispositionHeader);
            String uploadedFileId = schemaRegistry.uploadFile(inputStream);
            response = WSUtils.respond(OK, SUCCESS, uploadedFileId, null);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @Timed
    @GET
    @Produces({"application/octet-stream", "application/json"})
    @Path("/files/download/{fileId}")
    public Response downloadFile(@PathParam("fileId") String fileId) {
        Response response;
        try {
            StreamingOutput streamOutput = WSUtils.wrapWithStreamingOutput(schemaRegistry.downloadFile(fileId));
            response = Response.ok(streamOutput).build();
            return response;
        } catch (FileNotFoundException e) {
            response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, fileId);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/serializers")
    @Timed
    public Response addSerializer(SerDesInfo schemaSerDesInfo) {
        Response response;
        try {
            Long serializerId = schemaRegistry.addSerDesInfo(schemaSerDesInfo);
            response = WSUtils.respond(OK, SUCCESS, serializerId);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @GET
    @Path("/serializers/{id}")
    @Timed
    public Response getSerializer(@PathParam("id") Long serializerId) {
        Response response;
        try {
            SerDesInfo serializerInfo = schemaRegistry.getSerDesInfo(serializerId);
            response = WSUtils.respond(OK, SUCCESS, serializerInfo);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @POST
    @Path("/deserializers")
    @Timed
    public Response addDeserializer(SerDesInfo schemaSerDesInfo) {
        Response response;
        try {
            Long serializerId = schemaRegistry.addSerDesInfo(schemaSerDesInfo);
            response = WSUtils.respond(OK, SUCCESS, serializerId);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @GET
    @Path("/deserializers/{id}")
    @Timed
    public Response getDeserializer(@PathParam("id") Long deserializerId) {
        Response response;
        try {
            SerDesInfo serializerInfo = schemaRegistry.getSerDesInfo(deserializerId);
            response = WSUtils.respond(OK, SUCCESS, serializerInfo);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @POST
    @Path("/schemas/mapping/{schemaMetadataId}/{serDesId}")
    @Timed
    public Response mapSerDes(@PathParam("schemaMetadataId") Long schemaMetadataId, @PathParam("serDesId") Long serDesId) {
        try {
            schemaRegistry.mapSerDesWithSchema(schemaMetadataId, serDesId);
            return WSUtils.respond(OK, SUCCESS, true);
        } catch (Exception ex) {
            return WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }
    }

    @POST
    @Path("/schemas/types/{type}/groups/{schemaGroup}/names/{name}/mapping/{serDesId}")
    @Timed
    public Response mapSerDes(@PathParam("type") String type,
                              @PathParam("schemaGroup") String schemaGroup,
                              @PathParam("name") String name,
                              @PathParam("serDesId") Long serDesId) {
        SchemaKey schemaKey = new SchemaKey(type, schemaGroup, name);
        Response response;
        try {
            SchemaInfo schemaInfoStorable = schemaRegistry.getSchemaMetadata(schemaKey);
            schemaRegistry.mapSerDesWithSchema(schemaInfoStorable.getId(), serDesId);
            response = WSUtils.respond(OK, SUCCESS, true);
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }


    @GET
    @Path("/schemas/{id}/deserializers/")
    @Timed
    public Response getDeserializers(@PathParam("id") Long schemaMetadataId) {
        Response response;
        try {
            Collection<SerDesInfo> deserializers = schemaRegistry.getSchemaDeserializers(schemaMetadataId);

            if (deserializers != null) {
                response = WSUtils.respond(OK, SUCCESS, deserializers);
            } else {
                response = WSUtils.respond(NOT_FOUND, ENTITY_NOT_FOUND, schemaMetadataId.toString());
            }
        } catch (Exception ex) {
            response = WSUtils.respond(INTERNAL_SERVER_ERROR, EXCEPTION, ex.getMessage());
        }

        return response;
    }

}
