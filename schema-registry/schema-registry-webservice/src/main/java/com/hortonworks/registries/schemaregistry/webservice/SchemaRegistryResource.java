/**
 * Copyright 2016-2019 Cloudera, Inc.
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

import com.cloudera.dim.atlas.AtlasPlugin;
import com.cloudera.dim.atlas.events.AtlasEventLogger;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.common.SchemaRegistryServiceInfo;
import com.hortonworks.registries.common.SchemaRegistryVersion;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.core.util.AuthenticationUtils;
import com.hortonworks.registries.schemaregistry.authorizer.exception.AuthorizationException;
import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exportimport.BulkUploadInputFormat;
import com.hortonworks.registries.schemaregistry.exportimport.UploadResult;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.schemaregistry.webservice.validator.JarInputStreamValidator;
import com.hortonworks.registries.schemaregistry.webservice.validator.exception.InvalidJarFileException;
import com.hortonworks.registries.storage.transaction.UnitOfWork;
import io.dropwizard.server.AbstractServerFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.SUCCESS;
import static com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry.ORDER_BY_FIELDS_PARAM_NAME;
import static com.hortonworks.registries.schemaregistry.SchemaBranch.MASTER_BRANCH;

/**
 * Schema Registry resource that provides schema registry REST service.
 */
@Path("/api/v1/schemaregistry")
@Api(value = "/api/v1/schemaregistry", description = "Endpoint for Schema Registry service")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryResource extends BaseRegistryResource {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryResource.class);
    public static final String THROW_ERROR_IF_EXISTS = "_throwErrorIfExists";
    public static final String THROW_ERROR_IF_EXISTS_LOWER_CASE = THROW_ERROR_IF_EXISTS.toLowerCase();

    // reserved as schema related paths use these strings
    private static final String[] RESERVED_NAMES = {"aggregate", "versions", "compatibility"};
    private final SchemaRegistryVersion schemaRegistryVersion;
    private final AuthorizationAgent authorizationAgent;
    private final JarInputStreamValidator jarInputStreamValidator;
    private final SchemaMetadataTypeValidator schemaMetadataTypeValidator;
    private final AuthenticationUtils authenticationUtils;
    private final RegistryConfiguration registryConfiguration;
    private final AtlasEventLogger atlasEventLogger;
    private final AtlasPlugin atlasPlugin;

    @Inject
    public SchemaRegistryResource(ISchemaRegistry schemaRegistry,
                                  AuthorizationAgent authorizationAgent,
                                  AuthenticationUtils authenticationUtils,
                                  JarInputStreamValidator jarInputStreamValidator,
                                  SchemaMetadataTypeValidator schemaMetadataTypeValidator,
                                  RegistryConfiguration registryConfiguration,
                                  AtlasEventLogger atlasEventLogger,
                                  AtlasPlugin atlasPlugin) {
        super(schemaRegistry);
        this.registryConfiguration = registryConfiguration;
        this.schemaRegistryVersion = SchemaRegistryServiceInfo.get().version();
        this.atlasEventLogger = atlasEventLogger;
        this.authorizationAgent = authorizationAgent;
        this.authenticationUtils = authenticationUtils;
        this.jarInputStreamValidator = jarInputStreamValidator;
        this.schemaMetadataTypeValidator = schemaMetadataTypeValidator;
        this.atlasPlugin = atlasPlugin;
    }

    @GET
    @Path("/version")
    @ApiOperation(value = "Get the version information of this Schema Registry instance",
            response = SchemaRegistryVersion.class,
            tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response getVersion(@Context UriInfo uriInfo) {
        return WSUtils.respondEntity(schemaRegistryVersion, Response.Status.OK);
    }

    @GET
    @Path("/schemaproviders")
    @ApiOperation(value = "Get list of registered Schema Providers",
            notes = "The Schema Registry supports different types of schemas, such as Avro, JSON etc. " + "" +
                    "A Schema Provider is needed for each type of schema supported by the Schema Registry. " +
                    "Schema Provider supports defining schema, serializing and deserializing data using the schema, " +
                    " and checking compatibility between different versions of the schema.",
            response = SchemaProviderInfo.class, responseContainer = "List",
            tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response getRegisteredSchemaProviderInfos(@Context UriInfo uriInfo) {
            Collection<SchemaProviderInfo> schemaProviderInfos = schemaRegistry.getSupportedSchemaProviders();
            return WSUtils.respondEntities(schemaProviderInfos, Response.Status.OK);
    }

    //TODO : Get all the versions across all the branches

    @GET
    @Path("/schemas/aggregated")
    @ApiOperation(value = "Get list of schemas by filtering with the given query parameters",
            response = AggregatedSchemaMetadataInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response listAggregatedSchemas(@QueryParam("name") String schemaName,
                                          @QueryParam("description") String schemaDescription,
                                          @ApiParam(value = "_orderByFields=[<field-name>,<a/d>,]*\na = ascending, d = descending\n" +
                                                  "Ordering can be by id, type, schemaGroup, name, compatibility, validationLevel, " +
                                                  "timestamp, description, evolve") @QueryParam("_orderByFields") 
                                              @DefaultValue("timestamp,d") String orderByFields,
                                          @QueryParam("id") String id,
                                          @QueryParam("type") String type,
                                          @QueryParam("schemaGroup") String schemaGroup,
                                          @QueryParam("validationLevel") String validationLevel,
                                          @QueryParam("compatibility") String compatibility,
                                          @QueryParam("evolve") String evolve,
                                          @Context SecurityContext securityContext) throws Exception {
            Map<String, String> filters = createFilterForSchema(Optional.ofNullable(schemaName), 
                    Optional.ofNullable(schemaDescription), Optional.ofNullable(orderByFields), Optional.ofNullable(id), 
                    Optional.ofNullable(type), Optional.ofNullable(schemaGroup), Optional.ofNullable(validationLevel), 
                    Optional.ofNullable(compatibility), Optional.ofNullable(evolve));
            Collection<AggregatedSchemaMetadataInfo> schemaMetadatas = authorizationAgent
                    .authorizeGetAggregatedSchemaList(authenticationUtils.getUserAndGroups(securityContext),
                            schemaRegistry.findAggregatedSchemaMetadata(filters));

            return WSUtils.respondEntities(schemaMetadatas, Response.Status.OK);
    }

    @OPTIONS
    public Response options(@Context HttpServletResponse response) {
        AbstractServerFactory serverFactory = (AbstractServerFactory) registryConfiguration.getServerFactory();
        Set<String> allowedMethods = serverFactory.getAllowedMethods();
        if (allowedMethods == null || allowedMethods.isEmpty()) {
            response.setHeader("Allow", "GET");
        } else {
            response.setHeader("Allow", String.join(", ", allowedMethods));
        }
        return Response.ok().build();
    }

    @GET
    @Path("/schemas/{name}/aggregated")
    @ApiOperation(value = "Get aggregated schema information for the given schema name",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getAggregatedSchemaInfo(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                            @Context SecurityContext securityContext) throws Exception {
        AggregatedSchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getAggregatedSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo != null) {
            schemaMetadataInfo = authorizationAgent
                    .authorizeGetAggregatedSchemaInfo(authenticationUtils.getUserAndGroups(securityContext),
                            schemaMetadataInfo);
            return WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
        } else {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
        }
    }

    @GET
    @Path("/schemas")
    @ApiOperation(value = "Get list of schema metadata by filtering with the given query parameters",
            response = SchemaMetadataInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response listSchemas(@QueryParam("name") String schemaName,
                                @QueryParam("description") String schemaDescription,
                                @ApiParam(value = "_orderByFields=[<field-name>,<a/d>,]*\na = ascending, d = descending\n" +
                                        "Ordering can be by id, type, schemaGroup, name, compatibility, validationLevel, timestamp, " +
                                        "description, evolve") @QueryParam("_orderByFields") @DefaultValue("timestamp,d") String orderByFields,
                                @QueryParam("id") String id,
                                @QueryParam("type") String type,
                                @QueryParam("schemaGroup") String schemaGroup,
                                @QueryParam("validationLevel") String validationLevel,
                                @QueryParam("compatibility") String compatibility,
                                @QueryParam("evolve") String evolve,
                                @Context SecurityContext securityContext) throws Exception {
            Map<String, String> filters = createFilterForSchema(Optional.ofNullable(schemaName), 
                    Optional.ofNullable(schemaDescription), Optional.ofNullable(orderByFields), Optional.ofNullable(id), 
                    Optional.ofNullable(type), Optional.ofNullable(schemaGroup), Optional.ofNullable(validationLevel), 
                    Optional.ofNullable(compatibility), Optional.ofNullable(evolve));

            Collection<SchemaMetadataInfo> schemaMetadatas = authorizationAgent
                    .authorizeFindSchemas(authenticationUtils.getUserAndGroups(securityContext), schemaRegistry.findSchemaMetadata(filters));

            return WSUtils.respondEntities(schemaMetadatas, Response.Status.OK);
    }

    @GET
    @Path("/search/schemas")
    @ApiOperation(value = "Search for schema metadata containing the given name and description",
            notes = "Search the schema metadata for given name and description, return a list of schema metadata that contain the field.",
            response = SchemaMetadataInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response findSchemas(@ApiParam(required = true) @QueryParam("name") String schemaName,
                                @QueryParam("description") String schemaDescription,
                                @ApiParam(value = "_orderByFields=[<field-name>,<a/d>,]*\na = ascending, d = descending\n" +
                                        "Ordering can be by id, type, schemaGroup, name, compatibility, validationLevel, timestamp, description," +
                                        "evolve\nRecommended value is: timestamp,d", required = true) 
                                @QueryParam("_orderByFields") String orderByFields,
                                @Context SecurityContext securityContext) throws Exception {
        
        MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();
        for (Map.Entry<String, String> entry : createFilterForSchema(Optional.ofNullable(schemaName), 
                Optional.ofNullable(schemaDescription), Optional.ofNullable(orderByFields), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty()).entrySet()) {
            queryParameters.add(entry.getKey(), entry.getValue());
        }
            Collection<SchemaMetadataInfo> schemaMetadataInfos = authorizationAgent
                    .authorizeFindSchemas(authenticationUtils.getUserAndGroups(securityContext), findSchemaMetadataInfos(queryParameters));
            return WSUtils.respondEntities(schemaMetadataInfos, Response.Status.OK);
    }

    @GET
    @Path("/search/schemas/aggregated")
    @ApiOperation(value = "Search for schemas containing the given name and description",
            notes = "Search the schemas for given name and description, return a list of schemas that contain the field.",
            response = AggregatedSchemaMetadataInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response findAggregatedSchemas(
            @ApiParam(value = "name of the schema", required = true) @QueryParam("name") String schemaName,
            @QueryParam("description") String schemaDescription,
            @ApiParam(required = true) @QueryParam("_orderByFields") @DefaultValue("timestamp,d") String orderByFields,
            @Context SecurityContext securityContext) throws Exception {
        MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();
        for (Map.Entry<String, String> entry : createFilterForSchema(Optional.ofNullable(schemaName), 
                Optional.ofNullable(schemaDescription), Optional.ofNullable(orderByFields), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty()).entrySet()) {
            queryParameters.add(entry.getKey(), entry.getValue());
        }
            Collection<SchemaMetadataInfo> schemaMetadataInfos = findSchemaMetadataInfos(queryParameters);
            List<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfos = new ArrayList<>();
            for (SchemaMetadataInfo schemaMetadataInfo : schemaMetadataInfos) {
                SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
                List<SerDesInfo> serDesInfos = new ArrayList<>(schemaRegistry.getSerDes(schemaMetadataInfo
                                                                                                .getSchemaMetadata()
                                                                                                .getName()));
                aggregatedSchemaMetadataInfos.add(
                        new AggregatedSchemaMetadataInfo(schemaMetadata,
                                                         schemaMetadataInfo.getId(),
                                                         schemaMetadataInfo.getTimestamp(),
                                                         schemaRegistry.getAggregatedSchemaBranch(schemaMetadata.getName()),
                                                         serDesInfos));
            }

            return WSUtils.respondEntities(authorizationAgent.authorizeGetAggregatedSchemaList(authenticationUtils.getUserAndGroups(securityContext),
                    aggregatedSchemaMetadataInfos),
                    Response.Status.OK);
    }

    @GET
    @Path("/search/schemas/fields")
    @ApiOperation(value = "Search for schemas containing the given field names",
            notes = "Search the schemas for given field names and return a list of schemas that contain the field.\n" +
                    "If no parameter added, returns all schemas as many times as they have fields.",
            response = SchemaVersionKey.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response findSchemasByFields(@QueryParam("name") String name,
                                        @QueryParam("fieldNamespace") String nameSpace,
                                        @QueryParam("type") String type,
                                        @Context SecurityContext securityContext) throws Exception {
        MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();
        for (Map.Entry<String, String> entry : createFilterForNamespace(Optional.ofNullable(name), 
                Optional.ofNullable(nameSpace), Optional.ofNullable(type)).entrySet()) {
            queryParameters.add(entry.getKey(), entry.getValue());
        }
            Collection<SchemaVersionKey> schemaVersionKeys = authorizationAgent
                    .authorizeFindSchemasByFields(authenticationUtils.getUserAndGroups(securityContext), schemaRegistry,
                            schemaRegistry.findSchemasByFields(buildSchemaFieldQuery(queryParameters)));

            return WSUtils.respondEntities(schemaVersionKeys, Response.Status.OK);
    }

    @POST
    @Path("/schemas")
    @ApiOperation(value = "Create a schema metadata if it does not already exist",
            notes = "Creates a schema metadata with the given schema information if it does not already exist." +
                    " A unique schema identifier is returned.",
            response = Long.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response addSchemaInfo(@ApiParam(value = "Schema to be added to the registry", required = true)
                                          SchemaMetadata schemaMetadata,
                                  @Context UriInfo uriInfo,
                                  @Context HttpHeaders httpHeaders,
                                  @Context SecurityContext securityContext) throws AuthorizationException {
        
                schemaMetadata.trim();
                checkValueAsNullOrEmpty("Schema name", schemaMetadata.getName());
                checkValueAsNullOrEmpty("Schema type", schemaMetadata.getType());
                checkValidNames(schemaMetadata.getName());

                boolean throwErrorIfExists = isThrowErrorIfExists(httpHeaders);
                final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
                authorizationAgent.authorizeSchemaMetadata(auth, schemaMetadata, Authorizer.AccessType.CREATE);

                final Long schemaId;

                // first we check if the schema metadata already exists
                SchemaMetadataInfo existingMeta = schemaRegistry.getSchemaMetadataInfo(schemaMetadata.getName());
                if (existingMeta != null) {
                    // if it does then we return its id
                    schemaId = existingMeta.getId();
                } else {
                    // otherwise the schema meta is created
                    schemaId = schemaRegistry.addSchemaMetadata(schemaMetadata, throwErrorIfExists);
                    atlasEventLogger.withAuth(auth).createMeta(schemaId);
                }
                return WSUtils.respondEntity(schemaId, Response.Status.CREATED);
    }

    @POST
    @Path("/schemas/{name}")
    @ApiOperation(value = "Updates schema information for the given schema name",
        response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response updateSchemaInfo(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName, 
                                     @ApiParam(value = "Schema to be added to the registry\nType of schema can be e.g. AVRO, JSON\n" +
                                             "Name should be the same as in body\nGroup of schema can be e.g. kafka, hive", required = true)
                                         SchemaMetadata schemaMetadata,
                                     @Context UriInfo uriInfo,
                                     @Context SecurityContext securityContext) throws Exception {
        if (!schemaMetadataTypeValidator.isValid(schemaMetadata.getType())) {
            LOG.error("SchemaMetadata type is invalid: {}", schemaMetadata);
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_WITH_MESSAGE, 
                    "SchemaMetadata type is invalid");
        }
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaMetadata(auth,
                    schemaRegistry,
                    schemaName,
                    Authorizer.AccessType.UPDATE);
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.updateSchemaMetadata(schemaName, schemaMetadata);
            if (schemaMetadataInfo != null) {
                atlasEventLogger.withAuth(auth).updateMeta(schemaMetadataInfo.getId());
                return WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
    }

    @GET
    @Path("/schemas/{name}")
    @ApiOperation(value = "Get schema information for the given schema name",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaInfo(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                  @Context SecurityContext securityContext) throws Exception {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaName);
            if (schemaMetadataInfo != null) {
                authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(securityContext),
                        schemaMetadataInfo, Authorizer.AccessType.READ);
                return WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
    }

    @GET
    @Path("/schemasById/{schemaId}")
    @ApiOperation(value = "Get schema information for a given schema identifier",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaInfo(@ApiParam(value = "Schema identifier", required = true) @PathParam("schemaId") Long schemaId,
                                  @Context SecurityContext securityContext) throws Exception {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaId);
            if (schemaMetadataInfo != null) {
                authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(securityContext),
                        schemaMetadataInfo, Authorizer.AccessType.READ);
                return WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaId.toString());
            }
    }

    @DELETE
    @Path("/schemas/{name}")
    @ApiOperation(value = "Delete a schema metadata and all related data", tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response deleteSchemaMetadata(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                        @Context UriInfo uriInfo,
                                        @Context SecurityContext securityContext) throws Exception {
            authorizationAgent.authorizeDeleteSchemaMetadata(authenticationUtils.getUserAndGroups(securityContext),
                    schemaRegistry,
                    schemaName);
            schemaRegistry.deleteSchema(schemaName);
            return WSUtils.respond(Response.Status.OK);
    }

    @POST
    @Path("/schemas/{name}/versions/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(value = "Register a new version of an existing schema by uploading schema version text",
            notes = "Registers the given schema version to schema with name if the given file content is not registered as a version for this " + 
                    "schema, and returns respective version number." + 
                    "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
            response = Integer.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response uploadSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name")
                                                String schemaName,
                                        @QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                        @ApiParam(value = "Schema version text file to be uploaded", required = true)
                                        @FormDataParam("file") final InputStream inputStream,
                                        @ApiParam(value = "Description about the schema version to be uploaded", required = true)
                                        @FormDataParam("description") final String description,
                                        @QueryParam("disableCanonicalCheck") @DefaultValue("false") Boolean disableCanonicalCheck,
                                        @Context UriInfo uriInfo,
                                        @Context SecurityContext securityContext) throws Exception {
            SchemaVersion schemaVersion = null;
                authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(securityContext), schemaRegistry,
                        schemaName, schemaBranchName, Authorizer.AccessType.CREATE);
                schemaVersion = new SchemaVersion(IOUtils.toString(inputStream, StandardCharsets.UTF_8), description);
                return addSchemaVersion(schemaBranchName,
                        schemaName,
                        schemaVersion,
                        disableCanonicalCheck,
                        uriInfo,
                        securityContext);
    }

    @POST
    @Path("/schemas/{name}/versions")
    @ApiOperation(value = "Register a new version of the schema",
            notes = "Registers the given schema version to schema with name if the given schemaText is not registered as a version for this " + 
                    "schema, and returns respective version number." +
                    "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
            response = Integer.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response addSchemaVersion(@ApiParam(required = true) @QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                     @ApiParam(value = "Schema name", required = true) @PathParam("name")
                                      String schemaName,
                                     @ApiParam(value = "Details about the schema, schemaText in one line", required = true)
                                      SchemaVersion schemaVersion,
                                     @QueryParam("disableCanonicalCheck") @DefaultValue("false") Boolean disableCanonicalCheck,
                                     @Context UriInfo uriInfo,
                                     @Context SecurityContext securityContext) throws Exception {

                LOG.info("adding schema version for name [{}] with [{}]", schemaName, schemaVersion);
                final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
                authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                        schemaName,
                        schemaBranchName,
                        Authorizer.AccessType.CREATE);

                SchemaIdVersion version = schemaRegistry.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, disableCanonicalCheck);
                atlasEventLogger.withAuth(auth).createVersion(version.getSchemaVersionId());
                return WSUtils.respondEntity(version.getVersion(), Response.Status.CREATED);
    }

    @GET
    @Path("/schemas/{name}/versions/latest")
    @ApiOperation(value = "Get the latest version of the schema for the given schema name",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getLatestSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                           @QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                           @Context SecurityContext securityContext) throws Exception {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getLatestEnabledSchemaVersionInfo(schemaBranchName, schemaName);
            if (schemaVersionInfo != null) {
                authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(securityContext),
                        schemaRegistry,
                        schemaName,
                        schemaBranchName,
                        Authorizer.AccessType.READ);
                return WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
    }

    @GET
    @Path("/schemas/{name}/versions")
    @ApiOperation(value = "Get all the versions of the schema for the given schema name)",
            response = SchemaVersionInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getAllSchemaVersions(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                         @QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                         @QueryParam("states") List<Byte> stateIds,
                                         @Context SecurityContext securityContext) throws Exception {
            Collection<SchemaVersionInfo> schemaVersionInfos = schemaRegistry.getAllVersions(schemaBranchName, schemaName, stateIds);
            if (schemaVersionInfos != null) {
                authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(securityContext),
                        schemaRegistry,
                        schemaName,
                        schemaBranchName,
                        Authorizer.AccessType.READ);
                return WSUtils.respondEntities(schemaVersionInfos, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
    }

    @GET
    @Path("/schemas/{name}/versions/{version}")
    @ApiOperation(value = "Get a version of the schema identified by the schema name",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaMetadata,
                                     @ApiParam(value = "version of the schema", required = true) @PathParam("version") Integer versionNumber,
                                     @Context SecurityContext securityContext) throws Exception {
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadata, versionNumber);
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(securityContext), schemaRegistry,
                    schemaVersionInfo, Authorizer.AccessType.READ);

            return WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
    }

    @GET
    @Path("/schemas/versionsById/{id}")
    @ApiOperation(value = "Get a version of the schema identified by the given version id",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaVersionById(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                         @Context SecurityContext securityContext) throws Exception {
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(versionId);
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(schemaIdVersion);
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(securityContext), schemaRegistry,
                    schemaIdVersion, Authorizer.AccessType.READ);
            return WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
    }

    @GET
    @Path("/schemas/versionsByFingerprint/{fingerprint}")
    @ApiOperation(value = "Get a version of the schema with the given fingerprint",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaVersionByFingerprint(@ApiParam(value = "fingerprint of the schema text", required = true) 
                                                      @PathParam("fingerprint") String fingerprint,
                                                  @Context SecurityContext securityContext) throws Exception {
            final SchemaVersionInfo schemaVersionInfo = schemaRegistry.findSchemaVersionByFingerprint(fingerprint);
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(securityContext), schemaRegistry,
                    schemaVersionInfo, Authorizer.AccessType.READ);

            return WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
    }

    @GET
    @Path("/schemas/versions/statemachine")
    @ApiOperation(value = "Get schema version life cycle states",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getSchemaVersionLifeCycleStates() {
            SchemaVersionLifecycleStateMachineInfo states = schemaRegistry.getSchemaVersionLifecycleStateMachineInfo();
            return WSUtils.respondEntity(states, Response.Status.OK);
    }

    @POST
    @Path("/schemas/versions/{id}/state/enable")
    @ApiOperation(value = "Enables version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response enableSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                 @Context SecurityContext securityContext) throws Exception {
        
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                    versionId, Authorizer.AccessType.UPDATE);
            schemaRegistry.enableSchemaVersion(versionId);
            return WSUtils.respondEntity(true, Response.Status.OK);
    }

    @POST
    @Path("/schemas/versions/{id}/state/disable")
    @ApiOperation(value = "Disables version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response disableSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                  @Context SecurityContext securityContext) throws SchemaNotFoundException, SchemaLifecycleException {
        
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                    versionId, Authorizer.AccessType.UPDATE);
            schemaRegistry.disableSchemaVersion(versionId);
            return WSUtils.respondEntity(true, Response.Status.OK);
    }

    @POST
    @Path("/schemas/versions/{id}/state/archive")
    @ApiOperation(value = "Archives version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response archiveSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                  @Context SecurityContext securityContext) throws SchemaNotFoundException, SchemaLifecycleException {
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                    versionId, Authorizer.AccessType.UPDATE);
            schemaRegistry.archiveSchemaVersion(versionId);
            return WSUtils.respondEntity(true, Response.Status.OK);
    }


    @POST
    @Path("/schemas/versions/{id}/state/delete")
    @ApiOperation(value = "Deletes version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response deleteSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                 @Context SecurityContext securityContext) throws SchemaNotFoundException, SchemaLifecycleException {
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                    versionId, Authorizer.AccessType.DELETE);
            schemaRegistry.deleteSchemaVersion(versionId);
            return WSUtils.respondEntity(true, Response.Status.OK);
    }

    @POST
    @Path("/schemas/versions/{id}/state/startReview")
    @ApiOperation(value = "Starts review version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response startReviewSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                      @Context SecurityContext securityContext) throws SchemaNotFoundException, SchemaLifecycleException {

            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                    versionId, Authorizer.AccessType.UPDATE);
            schemaRegistry.startSchemaVersionReview(versionId);
            return WSUtils.respondEntity(true, Response.Status.OK);
    }

    @POST
    @Path("/schemas/versions/{id}/state/{stateId}")
    @ApiOperation(value = "Runs the state execution for schema version identified by the given version id " +
            "and executes action associated with target state id", response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response executeState(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                 @ApiParam(value = "stateId can be the name or id of the target state of the schema\nMore information about the " +
                                         "states can be accessed at /api/v1/schemaregistry/schemas/versions/statemachine", required = true) 
                                 @PathParam("stateId") Byte stateId,
                                 byte [] transitionDetails,
                                 @Context SecurityContext securityContext) throws SchemaNotFoundException, SchemaLifecycleException {
        
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                    versionId, Authorizer.AccessType.UPDATE);
            schemaRegistry.transitionState(versionId, stateId, transitionDetails);
            return WSUtils.respondEntity(true, Response.Status.OK);
    }

    @POST
    @Path("/schemas/{name}/compatibility")
    @ApiOperation(value = "Checks if the given schema text is compatible with all the versions of the schema identified by the name",
            response = CompatibilityResult.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response checkCompatibilityWithSchema(@QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                                 @ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                                 @ApiParam(value = "schema text to be checked for compatibility", required = true) String schemaText,
                                                 @Context SecurityContext securityContext) throws SchemaNotFoundException {

            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry, schemaName,
                    schemaBranchName, Authorizer.AccessType.READ);
            CompatibilityResult compatibilityResult = schemaRegistry.checkCompatibility(schemaBranchName, schemaName, schemaText);
            return WSUtils.respondEntity(compatibilityResult, Response.Status.OK);
    }

    @GET
    @Path("/schemas/{name}/serdes")
    @ApiOperation(value = "Get list of Serializers registered for the given schema name",
            response = SerDesInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response getSerializers(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                   @Context SecurityContext securityContext) {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaName);
            if (schemaMetadataInfo != null) {
                authorizationAgent.authorizeGetSerializers(authenticationUtils.getUserAndGroups(securityContext), schemaMetadataInfo);
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSerDes(schemaMetadataInfo.getSchemaMetadata().getName());
                return WSUtils.respondEntities(schemaSerializers, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/files")
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Upload the given file and returns respective identifier.", response = String.class, tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response uploadFile(@FormDataParam("file") final InputStream inputStream,
                               @FormDataParam("file") final FormDataContentDisposition contentDispositionHeader,
                               @Context SecurityContext securityContext) throws InvalidJarFileException, IOException {
            LOG.info("Received contentDispositionHeader: [{}]", contentDispositionHeader);
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSerDes(auth, Authorizer.AccessType.UPDATE);
            InputStream validatedStream = jarInputStreamValidator.validate(inputStream);
            String uploadedFileId = schemaRegistry.uploadFile(validatedStream);
            return WSUtils.respondEntity(uploadedFileId, Response.Status.OK);
    }

    @GET
    @Produces({"application/octet-stream", "application/json"})
    @Path("/files/download/{fileId}")
    @ApiOperation(value = "Downloads the respective for the given fileId if it exists", 
            response = StreamingOutput.class, tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response downloadFile(@ApiParam(value = "Identifier of the file (with extension) to be downloaded", required = true) 
                                     @PathParam("fileId") String fileId,
                                 @Context SecurityContext securityContext) throws IOException {

            authorizationAgent.authorizeSerDes(authenticationUtils.getUserAndGroups(securityContext), Authorizer.AccessType.READ);
            StreamingOutput streamOutput = WSUtils.wrapWithStreamingOutput(schemaRegistry.downloadFile(fileId));
        return Response.ok(streamOutput).build();
             
    }

    @POST
    @Path("/serdes")
    @ApiOperation(value = "Add a Serializer/Deserializer into the Schema Registry", response = Long.class, tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response addSerDes(@ApiParam(value = "Serializer/Deserializer information to be registered", required = true) @Valid SerDesPair serDesPair,
                              @Context UriInfo uriInfo,
                              @Context SecurityContext securityContext) {
        return addSerDesInfo(serDesPair, securityContext);
    }

    @GET
    @Path("/serdes/{id}")
    @ApiOperation(value = "Get a Serializer for the given serializer id", response = SerDesInfo.class, tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response getSerDes(@ApiParam(value = "Serializer identifier", required = true) @PathParam("id") Long serializerId,
                              @Context SecurityContext securityContext) {
            authorizationAgent.authorizeSerDes(authenticationUtils.getUserAndGroups(securityContext), Authorizer.AccessType.READ);
            SerDesInfo serializerInfo = schemaRegistry.getSerDes(serializerId);
            if (serializerInfo != null) {
                return WSUtils.respondEntity(serializerInfo, Response.Status.OK);
            } else {
                LOG.error("Ser/Des not found with id: " + serializerId);
                return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, serializerId.toString());
            }
    }

    private Response addSerDesInfo(SerDesPair serDesInfo, SecurityContext securityContext) {
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeSerDes(auth, Authorizer.AccessType.CREATE);
            Long serializerId = schemaRegistry.addSerDes(serDesInfo);
            return WSUtils.respondEntity(serializerId, Response.Status.OK);
    }

    @POST
    @Path("/schemas/{name}/mapping/{serDesId}")
    @ApiOperation(value = "Bind the given Serializer/Deserializer to the schema identified by the schema name", tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response mapSchemaWithSerDes(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                        @ApiParam(value = "Serializer/deserializer identifier", required = true) @PathParam("serDesId") Long serDesId,
                                        @Context UriInfo uriInfo,
                                        @Context SecurityContext securityContext) throws SchemaNotFoundException {
                final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
                authorizationAgent.authorizeMapSchemaWithSerDes(auth, schemaRegistry, schemaName);
                schemaRegistry.mapSchemaWithSerDes(schemaName, serDesId);
                return WSUtils.respondEntity(true, Response.Status.OK);
    }

    @DELETE
    @Path("/schemas/{name}/versions/{version}")
    @ApiOperation(value = "Delete a schema version given its schema name and version id", tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response deleteSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                        @ApiParam(value = "version of the schema", required = true) @PathParam("version") Integer versionNumber,
                                        @Context UriInfo uriInfo,
                                        @Context SecurityContext securityContext) throws SchemaNotFoundException, SchemaLifecycleException {
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, versionNumber);
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(securityContext), schemaRegistry,
                    schemaVersionKey, Authorizer.AccessType.DELETE);
            schemaRegistry.deleteSchemaVersion(schemaVersionKey);
            return WSUtils.respond(Response.Status.OK);
    }

    @GET
    @Path("/schemas/{name}/branches")
    @ApiOperation(value = "Get list of registered schema branches",
            response = SchemaBranch.class, responseContainer = "List",
            tags = OPERATION_GROUP_OTHER)
    @Timed
    @UnitOfWork
    public Response getAllBranches(@ApiParam(value = "Details about schema name", required = true) @PathParam("name") String schemaName,
                                   @Context UriInfo uriInfo,
                                   @Context SecurityContext securityContext) throws SchemaNotFoundException {
            Collection<SchemaBranch> schemaBranches = authorizationAgent.authorizeGetAllBranches(authenticationUtils.getUserAndGroups(securityContext),
                    schemaRegistry, schemaName, schemaRegistry.getSchemaBranches(schemaName));
            return WSUtils.respondEntities(schemaBranches, Response.Status.OK);
    }

    @POST
    @Path("/schemas/versionsById/{versionId}/branch")
    @ApiOperation(value = "Fork a new schema branch given its schema name and version id",
            response = SchemaBranch.class,
            tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response createSchemaBranch(@ApiParam(value = "Details about schema version", required = true) 
                                           @PathParam("versionId") Long schemaVersionId,
                                        @ApiParam(value = "Schema Branch Name", required = true) SchemaBranch schemaBranch,
                                        @Context SecurityContext securityContext) throws SchemaNotFoundException, SchemaBranchAlreadyExistsException {
            LOG.debug("Create branch \"{}\" for version with id {}", schemaBranch.getName(), schemaVersionId);
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.fetchSchemaVersionInfo(schemaVersionId);

            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeCreateSchemaBranch(auth,
                    schemaRegistry,
                    schemaVersionInfo.getSchemaMetadataId(),
                    schemaVersionId,
                    schemaBranch.getName());
            SchemaBranch createdSchemaBranch = schemaRegistry.createSchemaBranch(schemaVersionId, schemaBranch);
            return WSUtils.respondEntity(createdSchemaBranch, Response.Status.OK);
    }

    @POST
    @Path("/schemas/{versionId}/merge")
    @ApiOperation(value = "Merge a schema version to master given its version id",
            response = SchemaVersionMergeResult.class,
            tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response mergeSchemaVersion(@ApiParam(value = "Details about schema version", required = true) 
                                           @PathParam("versionId") Long schemaVersionId,
                                       @QueryParam("disableCanonicalCheck") @DefaultValue("false") Boolean disableCanonicalCheck,
                                       @Context SecurityContext securityContext) throws SchemaNotFoundException, IncompatibleSchemaException {
            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeMergeSchemaVersion(auth, schemaRegistry, schemaVersionId);
            SchemaVersionMergeResult schemaVersionMergeResult = schemaRegistry.mergeSchemaVersion(schemaVersionId, disableCanonicalCheck);
            return WSUtils.respondEntity(schemaVersionMergeResult, Response.Status.OK);
    }

    @DELETE
    @Path("/schemas/branch/{branchId}")
    @ApiOperation(value = "Delete a branch given its branch id", tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response deleteSchemaBranch(@ApiParam(value = "ID of the Schema Branch", required = true) @PathParam("branchId") Long schemaBranchId,
                                       @Context SecurityContext securityContext) throws InvalidSchemaBranchDeletionException {
            authorizationAgent.authorizeDeleteSchemaBranch(authenticationUtils.getUserAndGroups(securityContext),
                    schemaRegistry, schemaBranchId);
            schemaRegistry.deleteSchemaBranch(schemaBranchId);
            return WSUtils.respond(Response.Status.OK);
    }


    // When ever SCHEMA_BRANCH or SCHEMA_VERSION is updated in one of the node in the cluster, 
    // then it will use this API to notify rest of the node in the
    // cluster to update their corresponding cache.
    // TODO: This API was introduced as a temporary solution to address HA requirements with cache synchronization. 
    //  A more permanent and stable fix should be incorporated.
    @POST
    @Path("/cache/{cacheType}/invalidate")
    @ApiOperation(value = "Address HA requirements with cache synchronization.")
    @UnitOfWork
    public Response invalidateCache(@ApiParam(value = "Cache Id to be invalidated", required = true) 
                                        @PathParam("cacheType") SchemaRegistryCacheType cacheType, 
                                    @ApiParam(value = "key") String keyString) {
            LOG.debug("RetryableBlock to invalidate cache : {} with key : {} accepted", cacheType.name(), keyString);
            schemaRegistry.invalidateCache(cacheType, keyString);
            return WSUtils.respond(Response.Status.OK);
    }

    @POST
    @Path("/import")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(value = "Bulk import schemas from a file",
            notes = "Upload a file containing multiple schemas. The schemas will be processed and added to Schema Registry. " +
                    "In case there is already existing data in Schema Registry, there might be ID collisions. You should " +
                    "define what to do in case of collisions (fail or ignore). To avoid issues, it is recommended to import " +
                    "schemas when the database is empty.",
            response = UploadResult.class, tags = OPERATION_GROUP_EXPORT_IMPORT)
    @Timed
    @UnitOfWork
    public Response uploadSchemaVersion(@ApiParam(value = "Imported file format. Can be 0 (Cloudera) or 1 (Confluent)", required = true)
                                        @QueryParam("format") @DefaultValue("0") String fileFormat,
                                        @ApiParam(value = "In case of errors, should the operation fail or should we continue processing the remaining rows")
                                        @QueryParam("failOnError") @DefaultValue("true") boolean failOnError,
                                        @ApiParam(value = "File to upload. Please make sure the file contains valid data.", required = true)
                                        @FormDataParam("file") final InputStream inputStream,
                                        @Context SecurityContext securityContext) throws IOException {

            BulkUploadInputFormat format;
            if (StringUtils.isBlank(fileFormat)) {
                format = BulkUploadInputFormat.CLOUDERA;
            } else if ("0".equals(fileFormat) || "CLOUDERA".equalsIgnoreCase(fileFormat)) {
                format = BulkUploadInputFormat.CLOUDERA;
            } else if ("1".equals(fileFormat) || "CONFLUENT".equalsIgnoreCase(fileFormat)) {
                format = BulkUploadInputFormat.CONFLUENT;
            } else {
                return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST, "Invalid file format.");
            }

            final Authorizer.UserAndGroups auth = authenticationUtils.getUserAndGroups(securityContext);
            authorizationAgent.authorizeBulkImport(auth);

            UploadResult uploadResult = schemaRegistry.bulkUploadSchemas(inputStream, failOnError, format);
            return WSUtils.respondEntity(uploadResult, Response.Status.OK);
    }

    @POST
    @Path("/setupAtlasModel")
    @ApiOperation(value = "Setup SchemaRegistry model in Atlas",
            notes = "This method should only be called once, during system initialization.",
            response = String.class, tags = OPERATION_GROUP_ATLAS)
    @Timed
    @UnitOfWork
    public Response setupAtlasModel() {
        atlasPlugin.setupAtlasModel();
        atlasPlugin.setupKafkaSchemaModel();
        return WSUtils.respondString(Response.Status.OK, SUCCESS);
    }

    @VisibleForTesting
    Map<String, String> createFilterForSchema(Optional<String> name,
                                              Optional<String> description,
                                              Optional<String> orderByFields,
                                              Optional<String> id,
                                              Optional<String> type,
                                              Optional<String> schemaGroup,
                                              Optional<String> validationLevel,
                                              Optional<String> compatibility,
                                              Optional<String> evolve) {
        Map<String, String> filters = new HashMap<>();
        name.ifPresent(n -> filters.put("name", n));
        description.ifPresent(d -> filters.put("description", d));
        orderByFields.ifPresent(o -> filters.put("_orderByFields", o));
        id.ifPresent(i -> filters.put("id", i));
        type.ifPresent(t -> filters.put("type", t));
        schemaGroup.ifPresent(s -> filters.put("schemaGroup", s));
        validationLevel.ifPresent(v -> filters.put("validationLevel", v));
        compatibility.ifPresent(c -> filters.put("compatibility", c));
        evolve.ifPresent(e -> filters.put("evolve", e));
        return filters;
    }

    @VisibleForTesting
    Map<String, String> createFilterForNamespace(Optional<String> name, Optional<String> fieldNamespace, Optional<String> type) {
        Map<String, String> filters = new HashMap<>();
        name.ifPresent(n -> filters.put("name", n));
        fieldNamespace.ifPresent(f -> filters.put("fieldNamespace", f));
        type.ifPresent(t -> filters.put("type", t));

        return filters;
    }

    @VisibleForTesting
    Collection<SchemaMetadataInfo> findSchemaMetadataInfos(MultivaluedMap<String, String> queryParameters) {
        Collection<SchemaMetadataInfo> schemaMetadataInfos;
        // name and description for now, complex queries are supported by backend and front end can send the json
        // query for those complex queries.
        if (queryParameters.containsKey(SchemaMetadataStorable.NAME)
                || queryParameters.containsKey(SchemaMetadataStorable.DESCRIPTION)) {

            String orderByFieldStr = queryParameters.getFirst(ORDER_BY_FIELDS_PARAM_NAME);

            schemaMetadataInfos = schemaRegistry.searchSchemas(queryParameters, Optional.ofNullable(orderByFieldStr));
        } else {
            schemaMetadataInfos = Collections.emptyList();
        }
        return schemaMetadataInfos;
    }

    @VisibleForTesting
    SchemaFieldQuery buildSchemaFieldQuery(MultivaluedMap<String, String> queryParameters) {
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

    private void checkValidNames(String name) {
        for (String reservedName : RESERVED_NAMES) {
            if (reservedName.equalsIgnoreCase(name)) {
                throw new IllegalArgumentException("schema name [" + reservedName + "] is reserved");
            }
        }
    }

    private boolean isThrowErrorIfExists(HttpHeaders httpHeaders) {
        List<String> values = httpHeaders.getRequestHeader(THROW_ERROR_IF_EXISTS);
        if (values != null) {
            values = httpHeaders.getRequestHeader(THROW_ERROR_IF_EXISTS_LOWER_CASE);
        }
        return values != null && !values.isEmpty() && Boolean.parseBoolean(values.get(0));
    }

}
