/*
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry.state;

import com.hortonworks.registries.schemaregistry.SchemaVersionKey;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * todo we should have the state flow of a specific version stored so that it can be seen how a specific version is
 * changes its state till it finally reaches the terminal states. Once it reaches the terminal state, those entries
 * can be removed after a configured time. This may be useful for looking at changes of the schema version history.
 */
public final class SchemaLifeCycleStates {

    public static final SchemaLifeCycleState INITIATED = new InitiatedState();
    public static final SchemaLifeCycleState START_REVIEW = new StartReviewState();
    public static final SchemaLifeCycleState CHANGES_REQUIRED = new ChangesRequiredState();
    public static final SchemaLifeCycleState REVIEWED = new ReviewedState();
    public static final SchemaLifeCycleState ENABLED = new EnabledState();
    public static final SchemaLifeCycleState DISABLED = new DisabledState();
    public static final SchemaLifeCycleState ARCHIVED = new ArchivedState();
    public static final SchemaLifeCycleState DELETED = new DeletedState();

    /**
     * Registry of states which contain inbuilt ones and customs states can be registered with {@link #register(SchemaLifeCycleState)}.
     */
    public static class Registry {
        private static final Registry REGISTRY = new Registry();

        ConcurrentMap<Byte, SchemaLifeCycleState> states = new ConcurrentHashMap<>();

        private Registry() {
            registerInBuiltStates();
        }

        private void registerInBuiltStates() {
            Field[] declaredFields = SchemaLifeCycleStates.class.getDeclaredFields();
            for (Field field : declaredFields) {
                if (Modifier.isFinal(field.getModifiers()) &&
                        Modifier.isStatic(field.getModifiers()) &&
                        SchemaLifeCycleState.class.isAssignableFrom(field.getType())) {
                    SchemaLifeCycleState state = null;
                    try {
                        state = (SchemaLifeCycleState) field.get(null);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    states.put(state.id(), state);
                }
            }
        }

        /**
         * @return the singleton instance of this registry.
         */
        public static Registry getInstance() {
            return REGISTRY;
        }

        /**
         * Registers the given state with REGISTRY.
         *
         * @param state state to be registered.
         * @throws IllegalArgumentException if the given state is already registered.
         */
        public void register(SchemaLifeCycleState state) {
            SchemaLifeCycleState prevState = states.putIfAbsent(state.id(), state);
            if(prevState == state) {
                throw new IllegalArgumentException("Given state is already registered as " + prevState);
            }
        }

        /**
         * @param stateId state id to be deregistered.
         * @return Returns true if the given stateId exists and deregistered, false otherwise.
         */
        public boolean deregister(Byte stateId) {
            return states.remove(stateId) != null;
        }

    }

    private static final class InitiatedState extends AbstractSchemaLifeCycleState {

        private InitiatedState() {
            super("INITIATED", (byte) 1, "Schema version is initialized, It can either go to review or enabled states.");
        }

        @Override
        public void startReview(SchemaLifeCycleContext context) {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().startReview(schemaVersionKey);
            context.setState(START_REVIEW);
        }

        @Override
        public void enable(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().enable(schemaVersionKey);
            context.setState(ENABLED);
        }
    }


    private static final class StartReviewState extends AbstractSchemaLifeCycleState {
        // take the review state and give both successful and failure states
        // it should invoke custom state and return to the given target state eventually

        private StartReviewState() {
            super("StartReview", (byte) 2, "Initiates the process for reviewing with the given custom state");
        }

        public void startReview(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            //
        }
    }


    private static final class ChangesRequiredState extends AbstractSchemaLifeCycleState {

        public ChangesRequiredState() {
            super("ChangesRequired", (byte) 3, "Requires changes to be done in this schema");
        }

        @Override
        public void startReview(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().enable(schemaVersionKey);
            context.setState(START_REVIEW);
        }

        @Override
        public void delete(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().delete(schemaVersionKey);
            context.setState(DELETED);
        }
    }


    private static final class ReviewedState extends AbstractSchemaLifeCycleState {

        private ReviewedState() {
            super("ReviewedState", (byte) 4, "This schema version is successfully reviewed");
        }

        @Override
        public void enable(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().enable(schemaVersionKey);
            context.setState(ENABLED);
        }

        @Override
        public void archive(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().archive(schemaVersionKey);
            context.setState(ARCHIVED);
        }
    }

    private static final class EnabledState extends AbstractSchemaLifeCycleState {

        private EnabledState() {
            super("Enabled", (byte) 5, "Schema version is enabled");
        }

        @Override
        public void disable(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().disable(schemaVersionKey);
            context.setState(DISABLED);
        }

        @Override
        public void archive(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().archive(schemaVersionKey);
            context.setState(ARCHIVED);
        }
    }

    private static final class DisabledState extends AbstractSchemaLifeCycleState {
        private DisabledState() {
            super("Disabled", (byte) 6, "Schema version is disabled");
        }

        @Override
        public void enable(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().enable(schemaVersionKey);
            context.setState(ENABLED);
        }

        @Override
        public void archive(SchemaLifeCycleContext context) throws SchemaLifeCycleException {
            SchemaVersionKey schemaVersionKey = context.getSchemaVersionKey();
            context.getSchemaRegistry().archive(schemaVersionKey);
            context.setState(ARCHIVED);
        }
    }

    private static final class ArchivedState extends AbstractSchemaLifeCycleState {
        // TERMINAL STATE
        private ArchivedState() {
            super("Archived", (byte) 7, "Schema is archived and it is a terminal state");
        }
    }

    private static final class DeletedState extends AbstractSchemaLifeCycleState {
        // TERMINAL STATE
        private DeletedState() {
            super("Deleted", (byte) 8, "Schema is deleted and it is a terminal state");
        }
    }

}
