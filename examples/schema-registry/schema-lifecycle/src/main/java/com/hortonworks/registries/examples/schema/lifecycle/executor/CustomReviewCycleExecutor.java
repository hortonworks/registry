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

package com.hortonworks.registries.examples.schema.lifecycle.executor;

import com.hortonworks.registries.examples.schema.lifecycle.state.CustomReviewCycleStates;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.CustomSchemaStateExecutor;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachine;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateTransition;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateTransitionListener;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.util.Map;

public class CustomReviewCycleExecutor implements CustomSchemaStateExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CustomReviewCycleExecutor.class);

    public void registerPeerReviewState(SchemaVersionLifecycleStateMachine.Builder builder) {
        builder.register(CustomReviewCycleStates.PEER_REVIEW_STATE);
    }

    public void registerTechnicalLeadReviewState(SchemaVersionLifecycleStateMachine.Builder builder) {
        builder.register(CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE);
        builder.transition(new SchemaVersionLifecycleStateTransition(CustomReviewCycleStates.PEER_REVIEW_STATE.getId(), CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE.getId(),
                CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE.getName(), CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE.getDescription()), (SchemaVersionLifecycleContext context) -> {
            // Plugin a custom code to trigger as a part of the state transition, here we just record the state change in the database
            LOG.debug("Making a transition from 'PEER REVIEW' to 'TECHNICAL LEAD REVIEW' state");
            transitionToState(context, CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE);
        });
    }

    public void registerReviewedState(SchemaVersionLifecycleStateMachine.Builder builder) {
        builder.transition(new SchemaVersionLifecycleStateTransition(CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE.getId(), SchemaVersionLifecycleStates.REVIEWED.getId(),
                SchemaVersionLifecycleStates.REVIEWED.getName(), SchemaVersionLifecycleStates.REVIEWED.getDescription()), (SchemaVersionLifecycleContext context) -> {
            // Plugin a custom code to trigger as a part of the state transition, here we just record the state change in the database
            LOG.debug("Making a transition from 'TECHNICAL LEAD REVIEW' to 'REVIEWED' state");
            transitionToState(context, SchemaVersionLifecycleStates.REVIEWED);
        });
    }

    public void registerRejectedState(SchemaVersionLifecycleStateMachine.Builder builder) {
        builder.register(CustomReviewCycleStates.REJECTED_REVIEW_STATE);
        builder.transition(new SchemaVersionLifecycleStateTransition(CustomReviewCycleStates.PEER_REVIEW_STATE.getId(), CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId(),
                CustomReviewCycleStates.REJECTED_REVIEW_STATE.getName(), CustomReviewCycleStates.REJECTED_REVIEW_STATE.getDescription()), (SchemaVersionLifecycleContext context) -> {
            // Plugin a custom code to trigger as a part of the state transition, here we just record the state change in the database
            LOG.debug("Making a transition from 'PEER REVIEW' to 'REJECTED' state");
            transitionToState(context, CustomReviewCycleStates.REJECTED_REVIEW_STATE);
        });
        builder.transition(new SchemaVersionLifecycleStateTransition(CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE.getId(), CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId(),
                CustomReviewCycleStates.REJECTED_REVIEW_STATE.getName(), CustomReviewCycleStates.REJECTED_REVIEW_STATE.getDescription()), (SchemaVersionLifecycleContext context) -> {
            // Plugin a custom code to trigger as a part of the state transition, here we just record the state change in the database
            LOG.debug("Making a transition from 'TECHNICAL LEAD REVIEW' to 'REJECTED' state");
            transitionToState(context, CustomReviewCycleStates.REJECTED_REVIEW_STATE);
        });
    }

    public void registerChangesRequiredState(SchemaVersionLifecycleStateMachine.Builder builder) {
        builder.transition(new SchemaVersionLifecycleStateTransition(CustomReviewCycleStates.PEER_REVIEW_STATE.getId(), SchemaVersionLifecycleStates.CHANGES_REQUIRED.getId(),
                SchemaVersionLifecycleStates.CHANGES_REQUIRED.getName(), SchemaVersionLifecycleStates.CHANGES_REQUIRED.getDescription()), (SchemaVersionLifecycleContext context) -> {
            // Plugin a custom code to trigger as a part of the state transition, here we just record the state change in the database
            LOG.debug("Making a transition from 'PEER REVIEW' to 'REJECTED' state");
            transitionToState(context, SchemaVersionLifecycleStates.CHANGES_REQUIRED);
        });
        builder.transition(new SchemaVersionLifecycleStateTransition(CustomReviewCycleStates.TECHNICAL_LEAD_REVIEW_STATE.getId(), SchemaVersionLifecycleStates.CHANGES_REQUIRED.getId(),
                        SchemaVersionLifecycleStates.CHANGES_REQUIRED.getName(), SchemaVersionLifecycleStates.CHANGES_REQUIRED.getDescription()),
                (SchemaVersionLifecycleContext context) -> {
                    // Plugin a custom code to trigger as a part of the state transition, here we just record the state change in the database
                    LOG.debug("Making a transition from 'TECHNICAL LEAD REVIEW' to 'REJECTED' state");
                    transitionToState(context, SchemaVersionLifecycleStates.CHANGES_REQUIRED);
                });
    }

    public void registerNotificationsWithSchemaEnabled(SchemaVersionLifecycleStateMachine.Builder builder, Map<String, ?> props) {
        builder.getTransitionsWithActions().entrySet().stream().
                filter(transitionAction -> transitionAction.getKey().getTargetStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).
                forEach(transitionAction -> builder.registerListener(transitionAction.getKey(), new SchemaVersionLifecycleStateTransitionListener() {
                    @Override
                    public void preStateTransition(SchemaVersionLifecycleContext context) {
                        LOG.debug("preStateTransition() does nothing for this state transition");
                    }

                    @Override
                    public void postStateTransition(SchemaVersionLifecycleContext context) {
                        LOG.debug("postStateTransition() calling external review service to notify the state transition");
                        Long schemaVersionId = context.getSchemaVersionId();
                        WebTarget webTarget = ClientBuilder.newClient().target(props.get("review.service.url").toString()).
                                path("/v1/transition/schema/"+schemaVersionId+"/notify");
                        webTarget.request().post(null);
                    }
                }));
    }

    private void transitionToState(SchemaVersionLifecycleContext context,
                                   SchemaVersionLifecycleState targetState) throws SchemaLifecycleException {
        if (context.getDetails() == null || context.getDetails().length == 0)
            throw new RuntimeException("This state transition should not be triggered through UI");
        context.setState(targetState);
        try {
            context.updateSchemaVersionState();
        } catch (SchemaNotFoundException e) {
            throw new SchemaLifecycleException(e);
        }
    }

    @Override
    public void init(SchemaVersionLifecycleStateMachine.Builder builder, Byte successStateId, Byte retryStateId, Map<String, ?> props) {
        registerPeerReviewState(builder);
        registerTechnicalLeadReviewState(builder);
        registerReviewedState(builder);
        registerRejectedState(builder);
        registerChangesRequiredState(builder);
        registerNotificationsWithSchemaEnabled(builder, props);
    }

    @Override
    public void executeReviewState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        schemaVersionLifecycleContext.setState(CustomReviewCycleStates.PEER_REVIEW_STATE);
        schemaVersionLifecycleContext.updateSchemaVersionState();
    }
}
