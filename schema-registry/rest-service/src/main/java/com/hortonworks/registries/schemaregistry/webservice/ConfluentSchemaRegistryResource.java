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

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.ha.LeadershipParticipant;
import com.hortonworks.registries.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaNameVersionId;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema Registry resource that provides schema registry REST service.
 */
@Path("/v1/confluent")
@Api(value = "/api/v1/confluent", description = "Endpoint for Confluent Schema Registry API compatible service")
@Produces(MediaType.APPLICATION_JSON)
public class ConfluentSchemaRegistryResource extends  BaseRegistryResource {
    private static final Logger LOG = LoggerFactory.getLogger(ConfluentSchemaRegistryResource.class);

    public ConfluentSchemaRegistryResource(ISchemaRegistry schemaRegistry, AtomicReference<LeadershipParticipant> leadershipParticipant) {
        super(schemaRegistry, leadershipParticipant);
    }

    // Hack: Adding number in front of sections to get the ordering in generated swagger documentation correct
    private static final String OPERATION_GROUP_SCHEMA = "1. Schema";
    private static final String OPERATION_GROUP_SERDE = "2. Serializer/Deserializer";
    private static final String OPERATION_GROUP_OTHER = "3. Other";


    @GET
    @Path("/schemas/ids/{id}")
    @ApiOperation(value = "Get schema iby id",
        response = Schema.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getSchemaById(@ApiParam(value = "SchemaVersion id", required = true) @PathParam("id") Long id) {
        Response response;
        try {
            String schemaText = schemaRegistry.getSchemaById(id);
            SchemaString schema = new SchemaString();
            schema.setSchema(schemaText);
            response = WSUtils.respondEntity(schema, Response.Status.OK);
        } catch (SchemaNotFoundException snfe) {
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, Long.toString(id));
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving Schema with id: [{}]", id, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
        return response;
    }


    @POST
    @Path("/subjects/{subject}")
    @ApiOperation(value = "Get schema information for the given schema name", response = Id.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response lookUpSubjectVersion(@ApiParam(value = "Schema subject", required = true) @PathParam("subject") String subject, 
                               @ApiParam(value = "The schema ", required = true) String schema) {
        Response response;
        try {
            SchemaNameVersionId schemaNameVersionId = schemaRegistry.getSchemaNameVersionId(subject, new SchemaString().fromJson(schema).getSchema());
            
            
            if (schemaNameVersionId != null) {
                response = WSUtils.respondEntity(new Schema(schemaNameVersionId.getName(), schemaNameVersionId.getVersion(), schemaNameVersionId.getId(), schemaNameVersionId.getSchemaText()), Response.Status.OK);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, subject);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving schema version with name: [{}]", subject, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }
    
    
    @POST
    @Path("/subjects/{subject}/versions")
    @ApiOperation(value = "Register a new version of the schema",
        notes = "Registers the given schema version to schema with name if the given schemaText is not registered as a version for this schema, " +
                "and returns respective unique id." +
                "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
        response = Id.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response registerSchema(@ApiParam(value = "subject", required = true) @PathParam("subject")
                                  String subject,
                              @ApiParam(value = "Details about the schema", required = true)
                                  String schema,
                              @Context UriInfo uriInfo) {
        LOG.info("registerSchema for [{}] is [{}]", subject);
        return handleLeaderAction(uriInfo, () -> {
            Response response;
            try {
                LOG.info("registerSchema for [{}] is [{}]", subject);
                Long schemaId = schemaRegistry.registerSchema(subject, new SchemaString().fromJson(schema).getSchema(), null);
                Id id = new Id();
                id.setId(schemaId);
                response = WSUtils.respondEntity(id, Response.Status.OK);
            } catch (InvalidSchemaException ex) {
                LOG.error("Invalid schema error encountered while adding subject [{}]", subject, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INVALID_SCHEMA, ex.getMessage());
            } catch (IncompatibleSchemaException ex) {
                LOG.error("Incompatible schema error encountered while adding subject [{}]", subject, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA, ex.getMessage());
            } catch (UnsupportedSchemaTypeException ex) {
                LOG.error("Unsupported schema type encountered while adding subject [{}]", subject, ex);
                response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE, ex.getMessage());
            } catch (Exception ex) {
                LOG.error("Encountered error while adding subject [{}]", subject, ex);
                response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
            }

            return response;
        });
    }
    
    public static class SchemaString {
        private String schema;

        public SchemaString() {
        }

        public SchemaString fromJson(String json) throws IOException {
            return (SchemaString) (new ObjectMapper()).readValue(json, SchemaString.class);
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
            if(this == o) {
                return true;
            } else if(o != null && this.getClass() == o.getClass()) {
                if(!super.equals(o)) {
                    return false;
                } else {
                    SchemaString that = (SchemaString)o;
                    if(this.schema != null) {
                        if(!this.schema.equals(that.schema)) {
                            return false;
                        }
                    } else if(that.schema != null) {
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
            result = 31 * result + (this.schema != null?this.schema.hashCode():0);
            return result;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{schema=" + this.schema + "}");
            return sb.toString();
        }

        public String toJson() throws IOException {
            return (new ObjectMapper()).writeValueAsString(this);
        }
    }

    public static class Id {
        private long id;

        public Id() {
        }

        public Id fromJson(String json) throws IOException {
            return (Id)(new ObjectMapper()).readValue(json, Id.class);
        }

        @JsonProperty("id")
        public long getId() {
            return this.id;
        }

        @JsonProperty("id")
        public void setId(long id) {
            this.id = id;
        }

        public String toJson() throws IOException {
            return (new ObjectMapper()).writeValueAsString(this);
        }
    }

    public static class Version {
        private int id;

        public Version() {
        }

        public Id fromJson(String json) throws IOException {
            return (Id)(new ObjectMapper()).readValue(json, Id.class);
        }

        @JsonProperty("id")
        public int getId() {
            return this.id;
        }

        @JsonProperty("id")
        public void setId(int id) {
            this.id = id;
        }

        public String toJson() throws IOException {
            return (new ObjectMapper()).writeValueAsString(this);
        }
    }

    public class Schema implements Comparable<Schema> {
        private String subject;
        private Integer version;
        private Long id;
        private String schema;

        public Schema(@JsonProperty("subject") String subject, @JsonProperty("version") Integer version, @JsonProperty("id") Long id, @JsonProperty("schema") String schema) {
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
            if(this == o) {
                return true;
            } else if(o != null && this.getClass() == o.getClass()) {
                Schema that = (Schema)o;
                return !this.subject.equals(that.subject)?false:(!this.version.equals(that.version)?false:(!this.id.equals(that.getId())?false:this.schema.equals(that.schema)));
            } else {
                return false;
            }
        }

        public int hashCode() {
            int result = this.subject.hashCode();
            result = 31 * result + this.version.intValue();
            result = 31 * result + this.id.intValue();
            result = 31 * result + this.schema.hashCode();
            return result;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{subject=" + this.subject + ",");
            sb.append("version=" + this.version + ",");
            sb.append("id=" + this.id + ",");
            sb.append("schema=" + this.schema + "}");
            return sb.toString();
        }

        public int compareTo(Schema that) {
            int result = this.subject.compareTo(that.subject);
            if(result != 0) {
                return result;
            } else {
                result = this.version.intValue() - that.version.intValue();
                return result;
            }
        }
    }
    
    public static class ErrorMessage {
        private int errorCode;
        private String message;

        public ErrorMessage(@JsonProperty("error_code") int errorCode, @JsonProperty("message") String message) {
            this.errorCode = errorCode;
            this.message = message;
        }

        @JsonProperty("error_code")
        public int getErrorCode() {
            return this.errorCode;
        }

        @JsonProperty("error_code")
        public void setErrorCode(int error_code) {
            this.errorCode = error_code;
        }

        @JsonProperty
        public String getMessage() {
            return this.message;
        }

        @JsonProperty
        public void setMessage(String message) {
            this.message = message;
        }
    }

}
