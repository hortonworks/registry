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

package com.hortonworks.registries.examples.schema.lifecycle.state;

import com.hortonworks.registries.schemaregistry.state.BaseSchemaVersionLifecycleState;

public class CustomReviewCycleStates {

    public static final PeerReviewState PEER_REVIEW_STATE = new PeerReviewState();
    public static final TechnicalLeadReviewState TECHNICAL_LEAD_REVIEW_STATE = new TechnicalLeadReviewState();
    public static final RejectedReviewState REJECTED_REVIEW_STATE = new RejectedReviewState();

    public static final class PeerReviewState extends BaseSchemaVersionLifecycleState {

        public PeerReviewState() {
            this("PeerReview", (byte) 33, "In this state Peers would review the schema version changes");
        }

        private PeerReviewState(String name, byte id, String description) {
            super(name, id, description);
        }
    }

    public static final class TechnicalLeadReviewState extends BaseSchemaVersionLifecycleState {

        public TechnicalLeadReviewState() {
            this("TechnicalLeadReview", (byte) 34, "In this state Technical lead would review the schema version changes");
        }

        private TechnicalLeadReviewState(String name, byte id, String description) {
            super(name, id, description);
        }
    }

    public static final class RejectedReviewState extends BaseSchemaVersionLifecycleState {
        public RejectedReviewState() {
            this("Rejected", (byte) 35, "If the changes to schema version would be permanently discarded");
        }

        private RejectedReviewState(String name, byte id, String description) {
            super(name, id, description);
        }
    }
}
