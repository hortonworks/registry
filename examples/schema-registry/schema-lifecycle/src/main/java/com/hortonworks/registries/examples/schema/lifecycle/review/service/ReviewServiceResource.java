/*
 * Copyright 2017 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.registries.examples.schema.lifecycle.review.service;

import com.codahale.metrics.annotation.Timed;
import com.hortonworks.registries.examples.schema.lifecycle.state.CustomReviewCycleStates;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import io.swagger.annotations.ApiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
public class ReviewServiceResource {

    private SchemaRegistryClient schemaRegistryClient;
    private static final Logger LOG = LoggerFactory.getLogger(ReviewServiceResource.class);

    public ReviewServiceResource(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Path("/transition/schema/{versionId}/notify")
    @POST
    @Timed
    public Response recordTransitionEnable(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
        LOG.info("<<==== Schema version info : {} has transitioned to enabled state ====>>", schemaVersionId);
        return Response.ok().build();
    }

    @Path("/review/peer/schema/{versionId}/accept")
    @POST
    @Timed
    public Response acceptPeerReview(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
        return transition(schemaVersionId, CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE.getId(), "Reviewed by :- A,B and C" );
    }

    @Path("/review/peer/schema/{versionId}/reject")
    @POST
    @Timed
    public Response declinePeerReview(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
        return transition(schemaVersionId, CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId(), "Review declined by - C" );
    }

    @Path("/review/peer/schema/{versionId}/modify")
    @POST
    @Timed
    public Response changesRequiredPeerReview(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
        return transition(schemaVersionId, SchemaVersionLifecycleStates.CHANGES_REQUIRED.getId(), "Changes requested by - B" );
    }

    @Path("/review/technical/schema/{versionId}/accept")
    @POST
    @Timed
    public Response declineTechnicalReview(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
        return transition(schemaVersionId, SchemaVersionLifecycleStates.REVIEWED.getId(), "Reviewed by :- M and N" );
    }

    @Path("/review/technical/schema/{versionId}/reject")
    @POST
    @Timed
    public Response acceptTechnicalReview(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
        return transition(schemaVersionId, CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId(), "Review declined by - M" );
    }

    @Path("/review/technical/schema/{versionId}/modify")
    @POST
    @Timed
    public Response changesRequiredTechnicalReview(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
       return transition(schemaVersionId, SchemaVersionLifecycleStates.CHANGES_REQUIRED.getId(), "Changes requested by - N" );
    }

    private Response transition(Long schemaVersionId, Byte targetStateId, String stateDetails) {
        try {
            schemaRegistryClient.transitionState(schemaVersionId, targetStateId, stateDetails.getBytes());
            return Response.ok().build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().build();
        }
    }
}
