/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.webservice;

import com.cloudera.dim.atlas.events.AtlasEventLogger;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.exception.ErrorCallback;
import com.hortonworks.registries.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.core.util.AuthorizationUtils;
import com.hortonworks.registries.schemaregistry.authorizer.exception.AuthorizationException;
import com.hortonworks.registries.schemaregistry.authorizer.exception.RangerException;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.storage.transaction.UnitOfWork;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Schema Registry resource that provides schema registry REST service.
 * This is used to support confluent serdes, and also third party integrations that support confluent schema registry api,
 * but yet to adopt registry's api.
 */
@Path("/api/v1/confluent")
@Api(value = "/api/v1/confluent", description = "Endpoint for Confluent Schema Registry API compatible service")
@Produces({MediaType.APPLICATION_JSON, "application/vnd.schemaregistry.v1+json"})
public class ConfluentSchemaRegistryCompatibleResource extends BaseRegistryResource {
    private static final Logger LOG = LoggerFactory.getLogger(ConfluentSchemaRegistryCompatibleResource.class);

    private static final String OPERATION_GROUP_CONFLUENT_SR = "5. Confluent Schema Registry compatible API";

    private final AuthorizationAgent authorizationAgent;
    private final AuthorizationUtils authorizationUtils;
    private final AtlasEventLogger atlasEventLogger;

    @Inject
    public ConfluentSchemaRegistryCompatibleResource(ISchemaRegistry schemaRegistry,
                                                     AuthorizationAgent authorizationAgent,
                                                     AuthorizationUtils authorizationUtils,
                                                     AtlasEventLogger atlasEventLogger) {
        super(schemaRegistry);

        this.authorizationAgent = authorizationAgent;
        this.authorizationUtils = authorizationUtils;
        this.atlasEventLogger = atlasEventLogger;
    }

    @GET
    @Path("/schemas/ids/{id}")
    @ApiOperation(value = "Get schema version by id",
            response = Schema.class, tags = OPERATION_GROUP_CONFLUENT_SR)
    @Timed
    @UnitOfWork
    public Response getSchemaById(@ApiParam(value = "schema version id", required = true) @PathParam("id") Long id,
                                  @Context SecurityContext securityContext) {
        return wrapper(() -> {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(id));
            if (schemaVersionInfo == null) {
                return schemaNotFoundError();
            }
            authorizationAgent.authorizeSchemaVersion(authorizationUtils.getUserAndGroups(securityContext), schemaRegistry,
                    schemaVersionInfo, Authorizer.AccessType.READ);

            SchemaString schema = new SchemaString();
            schema.setSchema(schemaVersionInfo.getSchemaText());
            return WSUtils.respondEntity(schema, Response.Status.OK);
        });
    }

    @GET
    @Path("/subjects")
    @ApiOperation(value = "Get all registered subjects",
            response = String.class, responseContainer = "List", tags = OPERATION_GROUP_CONFLUENT_SR)
    @Timed
    @UnitOfWork
    public Response getSubjects(@Context SecurityContext securityContext) {
        return wrapper(() -> {
            List<String> registeredSubjects = authorizationAgent.authorizeFindSchemas(authorizationUtils.getUserAndGroups(securityContext),
                    schemaRegistry.findSchemaMetadata(Collections.emptyMap()))
                    .stream()
                    .map(x -> x.getSchemaMetadata().getName())
                    .collect(Collectors.toList());

            return WSUtils.respondEntity(registeredSubjects, Response.Status.OK);
        });
    }

    public static class ErrorMessage {
        private int errorCode;
        private String message;

        public ErrorMessage() {
        }

        public ErrorMessage(int errorCode, String message) {
            this.errorCode = errorCode;
            this.message = message;
        }

        @JsonProperty("error_code")
        public int getErrorCode() {
            return errorCode;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "ErrorMessage{" +
                    "errorCode=" + errorCode +
                    ", message='" + message + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ErrorMessage that = (ErrorMessage) o;

            if (errorCode != that.errorCode) {
                return false;
            }
            return message != null ? message.equals(that.message) : that.message == null;
        }

        @Override
        public int hashCode() {
            int result = errorCode;
            result = 31 * result + (message != null ? message.hashCode() : 0);
            return result;
        }
    }


    @GET
    @Path("/subjects/{subject}/versions")
    @ApiOperation(value = "Get the number of all schema versions of given subject",
            response = Integer.class, responseContainer = "List", tags = OPERATION_GROUP_CONFLUENT_SR)
    @Timed
    @UnitOfWork
    public Response getAllVersions(@ApiParam(value = "subject", required = true)
                                   @PathParam("subject")
                                           String subject,
                                   @Context SecurityContext securityContext) {
        return wrapper(() -> {
            List<Integer> registeredSubjects = authorizationAgent.authorizeGetAllVersions(authorizationUtils.getUserAndGroups(securityContext),
                    schemaRegistry,
                    schemaRegistry.getAllVersions(subject))
                    .stream()
                    .map(SchemaVersionInfo::getVersion)
                    .collect(Collectors.toList());

            return WSUtils.respondEntity(registeredSubjects, Response.Status.OK);
        });
        }

    private Response wrapper(ErrorCallback method) {
    try {
        return method.call();
    } catch (Exception e) {
        if (e instanceof AuthorizationException) {
            LOG.debug("Access denied. ", e);
            return WSUtils.confluentRespond(Response.Status.FORBIDDEN, CatalogResponse.ResponseMessage.ACCESS_DENIED, e.getMessage());
        } else if (e instanceof InvalidSchemaException) {
            LOG.error("Given schema is invalid", e);
            return invalidSchemaError();
        } else if (e instanceof IncompatibleSchemaException) {
            LOG.error("Incompatible schema error encountered while adding subject. ", e);
            return incompatibleSchemaError();
        } else if (e instanceof UnsupportedSchemaTypeException) {
            LOG.error("Unsupported schema type encountered while adding subject. ", e);
            return incompatibleSchemaError();
        } else if (e instanceof SchemaNotFoundException) {
            LOG.error("No schema found. ", e);
            return subjectNotFoundError();
        } else if (e instanceof RangerException) {
            return WSUtils.confluentRespond(Response.Status.BAD_GATEWAY, CatalogResponse.ResponseMessage.EXTERNAL_ERROR, e.getMessage());
        } else {
            LOG.error("Encountered error while retrieving all subjects", e);
            return serverError();
    }
    }
    }

    @GET
    @Path("/subjects/{subject}/versions/{versionId}")
    @ApiOperation(value = "Get the schema information for given subject and versionId",
            response = Integer.class, responseContainer = "List", tags = OPERATION_GROUP_CONFLUENT_SR)
    @Timed
    @UnitOfWork
    public Response getSchemaVersion(@ApiParam(value = "subject", required = true)
                                     @PathParam("subject")
                                             String subject,
                                     @ApiParam(value = "versionId", required = true)
                                     @PathParam("versionId")
                                             String versionId,
                                     @Context SecurityContext securityContext) {
        return wrapper(() -> {
            SchemaVersionInfo schemaVersionInfo = null;
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(subject);
            if ("latest".equals(versionId)) {
                schemaVersionInfo = schemaRegistry.getLatestSchemaVersionInfo(subject);
            } else {
                if (schemaMetadataInfo == null) {
                    throw new SchemaNotFoundException(subject);
                }
                SchemaVersionInfo fetchedSchemaVersionInfo = null;
                try {
                    Integer version = Integer.valueOf(versionId);
                    if (version > 0 && version <= Integer.MAX_VALUE) {
                        fetchedSchemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(subject, version));
                    } else {
                        LOG.error("versionId is not in valid range [{}, {}] ", 1, Integer.MAX_VALUE);
                    }
                } catch (NumberFormatException e) {
                    LOG.error("Invalid version id string ", versionId, e);
                } catch (SchemaNotFoundException e) {
                    LOG.error("Schema version not found with version id [{}]", versionId, e);
                }

                if (fetchedSchemaVersionInfo != null) {
                    if (subject.equals(fetchedSchemaVersionInfo.getName())) {
                        schemaVersionInfo = fetchedSchemaVersionInfo;
                    } else {
                        LOG.error("Received schema version for id [{}] belongs to subject [{}] which is different from requested subject [{}]",
                                  versionId, fetchedSchemaVersionInfo.getName(), subject);
                    }
                }
            }

            if (schemaVersionInfo == null) {
                return versionNotFoundError();
            } else {
                authorizationAgent.authorizeSchemaVersion(authorizationUtils.getUserAndGroups(securityContext), schemaRegistry,
                        schemaVersionInfo, Authorizer.AccessType.READ);
                Schema schema = new Schema(schemaVersionInfo.getName(),
                                                                               schemaVersionInfo.getVersion(),
                                                                               schemaVersionInfo.getId(),
                                                                               schemaVersionInfo.getSchemaText());
                return WSUtils.respondEntity(schema, Response.Status.OK);
            }
        });
    }

    @POST
    @Path("/subjects/{subject}")
    @ApiOperation(value = "Get schema information for the given schema subject and schema text", 
            response = Schema.class, 
            tags = OPERATION_GROUP_CONFLUENT_SR)
    @Timed
    @UnitOfWork
    public Response lookupSubjectVersion(@ApiParam(value = "Schema subject", required = true) @PathParam("subject") String subject,
                                         @ApiParam(value = "Confluent Schema Registry compatible schema text in one line", required = true) 
                                                 String schema,
                                         @Context SecurityContext securityContext) {
        return wrapper(() -> {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(subject, schemaStringFromJson(schema).getSchema());

            if (schemaVersionInfo != null) {
                authorizationAgent.authorizeSchemaVersion(authorizationUtils.getUserAndGroups(securityContext), schemaRegistry,
                        schemaVersionInfo, Authorizer.AccessType.READ);
                return WSUtils.respondEntity(new Schema(schemaVersionInfo.getName(), 
                        schemaVersionInfo.getVersion(), schemaVersionInfo.getId(), schemaVersionInfo.getSchemaText()), Response.Status.OK);
            } else {
                return schemaNotFoundError();
            }
        }
        );
    }

    @POST
    @Path("/subjects/{subject}/versions")
    @ApiOperation(value = "Register a new version of the schema",
            notes = "Registers the given schema version to schema with subject if the given schemaText is not registered as a version for " + 
                    "this schema, and returns respective unique id." + 
                    "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
            response = Id.class, tags = OPERATION_GROUP_CONFLUENT_SR)
    @Timed
    @UnitOfWork
    public Response registerSchemaVersion(@ApiParam(value = "subject", required = true) @PathParam("subject")
                                           String subject,
                                   @ApiParam(value = "Confluent Schema Registry compatible schema text in one line", required = true)
                                           String schema,
                                   @Context UriInfo uriInfo,
                                   @Context SecurityContext securityContext) {

        return wrapper(() -> {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(subject);
            Authorizer.UserAndGroups auth = authorizationUtils.getUserAndGroups(securityContext);
            if (schemaMetadataInfo == null) {
                SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(subject)
                        .type(AvroSchemaProvider.TYPE)
                        .schemaGroup("Kafka")
                        .build();

                authorizationAgent.authorizeSchemaMetadata(auth, schemaMetadata, Authorizer.AccessType.CREATE);

                Long schemaId = schemaRegistry.addSchemaMetadata(schemaMetadata);
                atlasEventLogger.withAuth(auth).createMeta(schemaId);

                schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(subject);
            }

            authorizationAgent.authorizeSchemaVersion(auth, schemaRegistry,
                    subject, SchemaBranch.MASTER_BRANCH, Authorizer.AccessType.CREATE);
            SchemaIdVersion schemaVersionInfo = schemaRegistry.addSchemaVersion(schemaMetadataInfo.getSchemaMetadata(),
                                                                                new SchemaVersion(schemaStringFromJson(schema).getSchema(), null));
            atlasEventLogger.withAuth(auth).createVersion(schemaVersionInfo.getSchemaVersionId());

            Id id = new Id();
            id.setId(schemaVersionInfo.getSchemaVersionId());
            return WSUtils.respondEntity(id, Response.Status.OK);

        });
    }

    @POST
    @Path("/compatibility/subjects/{schema}/versions/{version}")
    @ApiOperation(value = "Checks if the given schema text is compatible with the specified " +
                "(\"latest\" or versionID) version of the schema identified by the name",
            response = CompatibilityResult.class, tags = OPERATION_GROUP_CONFLUENT_SR)
    @Timed
    @UnitOfWork
    public Response checkCompatibilityWithSchema(@PathParam("schema") @NotNull String subject,
                                                 @PathParam("version") @NotNull String versionId,
                                                 @ApiParam(value = "schema text to be checked for compatibility", required = true) String schemaText,
                                                 @Context SecurityContext securityContext) {
        return wrapper(() -> {
            SchemaVersionInfo schemaVersionInfo = null;
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(subject);
            if (schemaMetadataInfo == null) {
                throw new SchemaNotFoundException(subject);
            }
            if ("latest".equals(versionId)) {
                schemaVersionInfo = schemaRegistry.getLatestSchemaVersionInfo(subject);
            } else {
                try {
                    Integer version = Integer.valueOf(versionId);
                    SchemaVersionInfo fetchedSchemaVersionInfo = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(subject, version));
                    if (fetchedSchemaVersionInfo != null && subject.equals(fetchedSchemaVersionInfo.getName())) {
                        schemaVersionInfo = fetchedSchemaVersionInfo;
                    } else {
                        LOG.error("Received schema version for id [{}] belongs to subject [{}] which is different from requested subject [{}]",
                                versionId, fetchedSchemaVersionInfo.getName(), subject);
                    }
                } catch (NumberFormatException e) {
                    LOG.error("Invalid version id string ", versionId, e);
                } catch (SchemaNotFoundException e) {
                    LOG.error("Schema version not found with version id [{}]", versionId, e);
                }
            }
            if (schemaVersionInfo == null) {
                return versionNotFoundError();
            } else {
                authorizationAgent.authorizeSchemaVersion(authorizationUtils.getUserAndGroups(securityContext), schemaRegistry,
                        schemaVersionInfo, Authorizer.AccessType.READ);
                CompatibilityResult compatibilityResult = schemaRegistry.checkCompatibility(subject, schemaStringFromJson(schemaText).getSchema());
                return WSUtils.respondEntity(compatibilityResult, Response.Status.OK);
            }
        });
    }

    public static Response serverError() {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                       .entity(new ErrorMessage(50001, "Error in the backend data store"))
                       .build();
    }

    public static Response subjectNotFoundError() {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(new ErrorMessage(40401, "Subject not found"))
                       .build();
    }

    public static Response versionNotFoundError() {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(new ErrorMessage(40402, "Version not found"))
                       .build();
    }

    public static Response schemaNotFoundError() {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(new ErrorMessage(40403, "Schema not found"))
                       .build();
    }

    public static Response invalidSchemaError() {
        return Response.status(422)
                       .entity(new ErrorMessage(42201, "Invalid Avro schema"))
                       .build();
    }

    public static Response incompatibleSchemaError() {
        return Response.status(Response.Status.CONFLICT)
                       .entity(new ErrorMessage(40901, "Incompatible Avro schema"))
                       .build();
    }

    private SchemaString schemaStringFromJson(String json) throws IOException {
        return new ObjectMapper().readValue(json, SchemaString.class);
    }


    public static class SchemaString {
        private String schema;

        public SchemaString() {
        }

        @JsonProperty("schema")
        public String getSchema() {
            return this.schema;
        }

        @JsonProperty("schema")
        public void setSchema(String schema) {
            this.schema = schema;
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && this.getClass() == o.getClass()) {
                if (!super.equals(o)) {
                    return false;
                } else {
                    SchemaString that = (SchemaString) o;
                    if (this.schema != null) {
                        if (!this.schema.equals(that.schema)) {
                            return false;
                        }
                    } else if (that.schema != null) {
                        return false;
                    }

                    return true;
                }
            } else {
                return false;
            }
        }

        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (this.schema != null ? this.schema.hashCode() : 0);
            return result;
        }

        public String toString() {
            return "{schema=" + this.schema + "}";
        }
    }


    public static class Id {
        private long id;

        public Id() {
        }

        @JsonProperty("id")
        public long getId() {
            return this.id;
        }

        @JsonProperty("id")
        public void setId(long id) {
            this.id = id;
        }

    }
    public static class Schema implements Comparable<Schema> {
        private String subject;
        private Integer version;
        private Long id;
        private String schema;

        public Schema(@JsonProperty("subject") String subject, 
                      @JsonProperty("version") Integer version, 
                      @JsonProperty("id") Long id, 
                      @JsonProperty("schema") String schema) {
            this.subject = subject;
            this.version = version;
            this.id = id;
            this.schema = schema;
        }

        @JsonProperty("subject")
        public String getSubject() {
            return this.subject;
        }

        @JsonProperty("subject")
        public void setSubject(String subject) {
            this.subject = subject;
        }

        @JsonProperty("version")
        public Integer getVersion() {
            return this.version;
        }

        @JsonProperty("version")
        public void setVersion(Integer version) {
            this.version = version;
        }

        @JsonProperty("id")
        public Long getId() {
            return this.id;
        }

        @JsonProperty("id")
        public void setId(Long id) {
            this.id = id;
        }

        @JsonProperty("schema")
        public String getSchema() {
            return this.schema;
        }

        @JsonProperty("schema")
        public void setSchema(String schema) {
            this.schema = schema;
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && this.getClass() == o.getClass()) {
                Schema that = (Schema) o;
                return this.subject.equals(that.subject)
                        && (this.version.equals(that.version)
                        && (this.id.equals(that.getId())
                        && this.schema.equals(that.schema)));
            } else {
                return false;
            }
        }

        public int hashCode() {
            int result = this.subject.hashCode();
            result = 31 * result + this.version;
            result = 31 * result + this.id.intValue();
            result = 31 * result + this.schema.hashCode();
            return result;
        }

        public String toString() {
            return ("{subject=" + this.subject + ",") +
                    "version=" + this.version + "," +
                    "id=" + this.id + "," +
                    "schema=" + this.schema + "}";
        }

        public int compareTo(Schema that) {
            int result = this.subject.compareTo(that.subject);
            if (result != 0) {
                return result;
            } else {
                result = this.version - that.version;
                return result;
            }
        }
    }

}
