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

import com.google.common.collect.Lists;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Schema version life cycle state flow of a specific version stored so that it can be seen how a specific version
 * changes its state till it finally reaches the terminal states. Once it reaches the terminal state, those entries
 * can be removed after a configured time. This may be useful for looking at changes of the schema version history.
 */
public final class SchemaVersionLifecycleStates {

    public static final InbuiltSchemaVersionLifecycleState INITIATED = new InitiatedState();
    public static final InbuiltSchemaVersionLifecycleState START_REVIEW = new StartReviewState();
    public static final InbuiltSchemaVersionLifecycleState CHANGES_REQUIRED = new ChangesRequiredState();
    public static final InbuiltSchemaVersionLifecycleState REVIEWED = new ReviewedState();
    public static final InbuiltSchemaVersionLifecycleState ENABLED = new EnabledState();
    public static final InbuiltSchemaVersionLifecycleState DISABLED = new DisabledState();
    public static final InbuiltSchemaVersionLifecycleState ARCHIVED = new ArchivedState();
    public static final InbuiltSchemaVersionLifecycleState DELETED = new DeletedState();
    public static final SchemaVersionLifecycleState CUSTOM_STATE = new CustomState();

    /**
     * Registry of states which contain inbuilt ones and customs states can be registered with {@link #register(SchemaVersionLifecycleState)}.
     */
    public static class Registry {

        ConcurrentMap<Byte, SchemaVersionLifecycleState> states = new ConcurrentHashMap<>();

        public Registry() {
            registerInBuiltStates();
        }

        private void registerInBuiltStates() {
            Field[] declaredFields = SchemaVersionLifecycleStates.class.getDeclaredFields();
            for (Field field : declaredFields) {
                if (Modifier.isFinal(field.getModifiers()) &&
                        Modifier.isStatic(field.getModifiers()) &&
                        InbuiltSchemaVersionLifecycleState.class.isAssignableFrom(field.getType())) {
                    InbuiltSchemaVersionLifecycleState state = null;
                    try {
                        state = (InbuiltSchemaVersionLifecycleState) field.get(null);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    register(state);
                }
            }
        }

        /**
         * Registers the given state with REGISTRY.
         *
         * @param state state to be registered.
         *
         * @throws IllegalArgumentException if the given state is already registered.
         */
        public void register(SchemaVersionLifecycleState state) {
            SchemaVersionLifecycleState prevState = states.putIfAbsent(state.id(), state);
            if (prevState == state) {
                throw new IllegalArgumentException("Given state is already registered as " + prevState);
            }
        }

        /**
         * @param stateId state id to be deregistered.
         *
         * @return Returns true if the given stateId exists and deregistered, false otherwise.
         */
        public boolean deregister(Byte stateId) {
            return states.remove(stateId) != null;
        }

        public SchemaVersionLifecycleState get(Byte stateId) {
            return states.get(stateId);
        }

    }

    private static final class InitiatedState extends AbstractInbuiltSchemaLifecycleState {

        private InitiatedState() {
            super("INITIATED", (byte) 1, "Schema version is initialized, It can either go to review or enabled states.", Lists
                    .newArrayList(START_REVIEW, ENABLED));
        }

        @Override
        public void startReview(SchemaVersionLifecycleContext context,
                                SchemaReviewExecutor schemaReviewExecutor)
                throws SchemaLifecycleException, SchemaNotFoundException {
            context.setState(START_REVIEW);
            // get review state executor and
            context.updateSchemaVersionState();
        }

        @Override
        public void enable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException, IncompatibleSchemaException {
            transitionToEnableState(context);
        }

        @Override
        public String toString() {
            return "InitiatedState{" + super.toString() + "}";
        }
    }

    private static final class StartReviewState extends AbstractInbuiltSchemaLifecycleState {
        // schemaReviewExecutor is given successful and failure states.
        // it should invoke custom state and return to success/failure state eventually.

        private StartReviewState() {
            super("StartReview", (byte) 2, "Initiates the process for reviewing with the given custom state", Collections
                    .singletonList(CUSTOM_STATE));
        }

        public void startReview(SchemaVersionLifecycleContext context,
                                SchemaReviewExecutor schemaReviewExecutor)
                throws SchemaLifecycleException, SchemaNotFoundException {
            schemaReviewExecutor.execute(context);
        }

        @Override
        public String toString() {
            return "StartReviewState{" + super.toString() + "}";
        }

    }

    private static final class ChangesRequiredState extends AbstractInbuiltSchemaLifecycleState {

        public ChangesRequiredState() {
            super("ChangesRequired",
                  (byte) 3,
                  "Requires changes to be done in this schema",
                  Lists.newArrayList(START_REVIEW, DELETED));
        }

        @Override
        public void startReview(SchemaVersionLifecycleContext context,
                                SchemaReviewExecutor schemaReviewExecutor)
                throws SchemaLifecycleException, SchemaNotFoundException {
            context.setState(START_REVIEW);
            // execute start review process, updation of the state should be done by schemaReviewExecutor
            schemaReviewExecutor.execute(context);
        }

        @Override
        public void delete(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            context.setState(DELETED);
            context.getSchemaVersionService().deleteSchemaVersion(context.getSchemaVersionId());
            context.updateSchemaVersionState();
        }

        @Override
        public String toString() {
            return "ChangesRequiredState{" + super.toString() + "}";
        }

    }

    private static final class ReviewedState extends AbstractInbuiltSchemaLifecycleState {

        private ReviewedState() {
            super("ReviewedState",
                  (byte) 4,
                  "This schema version is successfully reviewed",
                  Lists.newArrayList(ENABLED, ARCHIVED));
        }

        @Override
        public void enable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException, IncompatibleSchemaException {
            transitionToEnableState(context);
        }

        @Override
        public void archive(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            context.setState(ARCHIVED);
            context.updateSchemaVersionState();
        }

        @Override
        public String toString() {
            return "ReviewedState{" + super.toString() + "}";
        }

    }

    private static final class EnabledState extends AbstractInbuiltSchemaLifecycleState {

        private EnabledState() {
            super("Enabled",
                  (byte) 5,
                  "Schema version is enabled",
                  Lists.newArrayList(DISABLED, ARCHIVED));
        }

        @Override
        public void disable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            context.setState(DISABLED);
            context.updateSchemaVersionState();
        }

        @Override
        public void archive(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            context.setState(ARCHIVED);
            context.updateSchemaVersionState();
        }

        @Override
        public String toString() {
            return "EnabledState{" + super.toString() + "}";
        }

    }

    private static final class DisabledState extends AbstractInbuiltSchemaLifecycleState {
        private DisabledState() {
            super("Disabled",
                  (byte) 6,
                  "Schema version is disabled",
                  Lists.newArrayList(ENABLED, ARCHIVED));
        }

        @Override
        public void enable(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException, IncompatibleSchemaException {
            transitionToEnableState(context);
        }

        @Override
        public void archive(SchemaVersionLifecycleContext context)
                throws SchemaLifecycleException, SchemaNotFoundException {
            context.setState(ARCHIVED);
            context.updateSchemaVersionState();
        }


        @Override
        public String toString() {
            return "DisabledState{" + super.toString() + "}";
        }

    }

    private static void checkCompatibility(SchemaVersionService schemaVersionService,
                                           SchemaMetadata schemaMetadata,
                                           String toSchemaText,
                                           String fromSchemaText) throws IncompatibleSchemaException {
        CompatibilityResult compatibilityResult = schemaVersionService.checkForCompatibility(schemaMetadata,
                                                                                             toSchemaText,
                                                                                             fromSchemaText);
        if (!compatibilityResult.isCompatible()) {
            String errMsg = String.format("Given schema is not compatible with latest schema versions. \n" +
                                                  "Error location: [%s] \n" +
                                                  "Error encountered is: [%s]",
                                          compatibilityResult.getErrorLocation(),
                                          compatibilityResult.getErrorMessage());
            throw new IncompatibleSchemaException(errMsg);
        }
    }

    private static final class ArchivedState extends AbstractInbuiltSchemaLifecycleState {
        // TERMINAL STATE
        private ArchivedState() {
            super("Archived",
                  (byte) 7,
                  "Schema is archived and it is a terminal state",
                  Collections.emptyList());
        }

        @Override
        public String toString() {
            return "ArchivedState{" + super.toString() + "}";
        }

    }

    private static final class DeletedState extends AbstractInbuiltSchemaLifecycleState {
        // TERMINAL STATE
        private DeletedState() {
            super("Deleted",
                  (byte) 8,
                  "Schema is deleted and it is a terminal state",
                  Collections.emptyList());
        }

        @Override
        public String toString() {
            return "DeletedState{" + super.toString() + "}";
        }

    }

    private static final class CustomState implements SchemaVersionLifecycleState {
        @Override
        public Byte id() {
            return Byte.MIN_VALUE;
        }

        @Override
        public String name() {
            return "custom";
        }

        @Override
        public String description() {
            return "Custom state at which user runs its own logic to move state";
        }
    }

    public static void transitionToEnableState(SchemaVersionLifecycleContext context)
            throws SchemaNotFoundException, IncompatibleSchemaException, SchemaLifecycleException {
        Long schemaVersionId = context.getSchemaVersionId();
        SchemaVersionService schemaVersionService = context.getSchemaVersionService();

        SchemaMetadataInfo schemaMetadataInfo = schemaVersionService.getSchemaMetadata(schemaVersionId);
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        String schemaName = schemaMetadata.getName();
        SchemaValidationLevel validationLevel = schemaMetadata.getValidationLevel();

        SchemaVersionInfo schemaVersionInfo = schemaVersionService.getSchemaVersionInfo(schemaVersionId);
        int schemaVersion = schemaVersionInfo.getVersion();
        String schemaText = schemaVersionInfo.getSchemaText();
        List<SchemaVersionInfo> allEnabledSchemaVersions =
                schemaVersionService.getAllSchemaVersions(schemaName)
                                    .stream()
                                    .filter(x -> SchemaVersionLifecycleStates.ENABLED.id().equals(x.getStateId()))
                                    .collect(Collectors.toList());

        if(!allEnabledSchemaVersions.isEmpty()) {
            if (validationLevel.equals(SchemaValidationLevel.ALL)) {
                for (SchemaVersionInfo curSchemaVersionInfo : allEnabledSchemaVersions) {
                    int curVersion = curSchemaVersionInfo.getVersion();
                    if (curVersion < schemaVersion) {
                        checkCompatibility(schemaVersionService,
                                           schemaMetadata,
                                           schemaText,
                                           curSchemaVersionInfo.getSchemaText());
                    } else {
                        checkCompatibility(schemaVersionService,
                                           schemaMetadata,
                                           curSchemaVersionInfo.getSchemaText(),
                                           schemaText);
                    }
                }
            } else if (validationLevel.equals(SchemaValidationLevel.LATEST)) {
                List<SchemaVersionInfo> sortedSchemaVersionInfos = new ArrayList<>(allEnabledSchemaVersions);
                sortedSchemaVersionInfos.sort(Comparator.comparingInt(SchemaVersionInfo::getVersion));
                int i = 0;
                int size = sortedSchemaVersionInfos.size();
                for (; i < size && sortedSchemaVersionInfos.get(i).getVersion() < schemaVersion; i++) {
                    String fromSchemaText = sortedSchemaVersionInfos.get(i).getSchemaText();
                    checkCompatibility(schemaVersionService, schemaMetadata, schemaText, fromSchemaText);
                }
                for (; i < size && sortedSchemaVersionInfos.get(i).getVersion() > schemaVersion; i++) {
                    String toSchemaText = sortedSchemaVersionInfos.get(i).getSchemaText();
                    checkCompatibility(schemaVersionService, schemaMetadata, toSchemaText, schemaText);
                }
            }
        }
        context.setState(ENABLED);
        context.updateSchemaVersionState();
    }

}
