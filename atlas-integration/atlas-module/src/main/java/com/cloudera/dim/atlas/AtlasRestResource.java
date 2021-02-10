/*
 * Copyright 2016-2021 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>SchemaRegistryClient
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.dim.atlas;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.registries.common.util.WSUtils;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.storage.transaction.UnitOfWork;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/** REST endpoint. */
@Path("/api/v2/atlas")
@Api(value = "/api/v2/atlas", description = "Endpoint for Schema Registry service")
@Produces(MediaType.APPLICATION_JSON)
public class AtlasRestResource {

    static final String OPERATION_GROUP_ATLAS = "6. Atlas";

    private final AtlasSchemaRegistry schemaRegistry;
    private final AuthorizationAgent authorizationAgent;
    private final SchemaMetadataTypeValidator schemaMetadataTypeValidator;

    public AtlasRestResource(AtlasSchemaRegistry schemaRegistry, AuthorizationAgent authorizationAgent,
                             SchemaMetadataTypeValidator schemaMetadataTypeValidator) {
        this.schemaRegistry = schemaRegistry;
        this.authorizationAgent = authorizationAgent;
        this.schemaMetadataTypeValidator = schemaMetadataTypeValidator;
    }

    @POST
    @Path("/setup")
    @ApiOperation(value = "Setup SchemaRegistry model in Atlas",
            notes = "This method should only be called, during system initialization.",
            response = Long.class, tags = OPERATION_GROUP_ATLAS)
    @Timed
    @UnitOfWork
    public Response setupAtlasModel() {
        schemaRegistry.setupAtlasModel();
        return WSUtils.respondEntity("Success", Response.Status.OK);
    }

}
