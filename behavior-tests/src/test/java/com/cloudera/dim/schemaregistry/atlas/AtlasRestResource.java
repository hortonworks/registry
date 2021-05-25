/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.cloudera.dim.schemaregistry.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path("/api/atlas/v2")
@Produces(MediaType.APPLICATION_JSON)
public class AtlasRestResource {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRestResource.class);

    @POST
    @Path("/bulk")
    public Response getVersion(AtlasEntity.AtlasEntitiesWithExtInfo request, @Context UriInfo uriInfo) {
        //return WSUtils.respondEntity(schemaRegistryVersion, Response.Status.OK);

        EntityMutationResponse response = new EntityMutationResponse();
        //response.setGuidAssignments();

        return Response.ok(response).build();
    }
}
