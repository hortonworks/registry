package com.cloudera.dim.atlas;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.registries.common.util.WSUtils;
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

    static final String OPERATION_GROUP_ATLAS = "5. Atlas";

    private final AtlasSchemaRegistry schemaRegistry;

    public AtlasRestResource(AtlasSchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
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
