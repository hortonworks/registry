/*
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
 */
package com.hortonworks.registries.schemaregistry.state;

import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;

/**
 *
 */
public class SchemaVersionLifecycleStatesTest {
    private static Logger LOG = LoggerFactory.getLogger(SchemaVersionLifecycleStatesTest.class);

    @Rule
    public TestName testName = new TestName();

    private SchemaVersionLifecycleContext context;

    @Before
    public void setup() {
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("schema-1").type("avro")
                                                                              .schemaGroup("kafka")
                                                                              .build();
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata, new Random().nextLong(), System.currentTimeMillis());
        SchemaVersionInfo schemaVersionInfo =
                new SchemaVersionInfo(new Random().nextLong(), schemaMetadata.getName(), 1, schemaMetadataInfo.getId(),
                                      "{\"type\":\"string\"}", System.currentTimeMillis(),
                                      "", SchemaVersionLifecycleStates.ENABLED.getId());

        SchemaVersionService schemaVersionServiceMock = new SchemaVersionService() {
            @Override
            public void updateSchemaVersionState(SchemaVersionLifecycleContext schemaLifeCycleContext) {
                LOG.info("Updating schema version: [{}]", schemaLifeCycleContext);
            }

            @Override
            public void deleteSchemaVersion(Long schemaVersionId) {
                LOG.info("Deleting schema version [{}]", schemaVersionId);
            }

            @Override
            public SchemaMetadataInfo getSchemaMetadata(long schemaVersionId) throws SchemaNotFoundException {
                return schemaMetadataInfo;
            }

            @Override
            public SchemaVersionInfo getSchemaVersionInfo(long schemaVersionId) throws SchemaNotFoundException {
                return schemaVersionInfo;
            }

            @Override
            public CompatibilityResult checkForCompatibility(SchemaMetadata schemaMetadata,
                                                             String toSchemaText,
                                                             String existingSchemaText) {
                return CompatibilityResult.SUCCESS;
            }

            @Override
            public Collection<SchemaVersionInfo> getAllSchemaVersions(String schemaBranchName, String schemaName) throws SchemaNotFoundException {
                return Collections.singletonList(schemaVersionInfo);
            }

        };
        SchemaVersionLifecycleStateMachine lifecycleStateMachine = SchemaVersionLifecycleStateMachine.newBuilder()
                                                                                                     .build();

        context = new SchemaVersionLifecycleContext(schemaVersionInfo.getId(),
                                                    1,
                                                    schemaVersionServiceMock,
                                                    lifecycleStateMachine,
                                                    new DefaultCustomSchemaStateExecutor());
    }

    @Test
    public void testInitiatedState() throws Exception {

        InbuiltSchemaVersionLifecycleState initiated = SchemaVersionLifecycleStates.INITIATED;

        DefaultCustomSchemaStateExecutor schemaReviewExecutor = createDefaultSchemaReviewExecutor();
        initiated.startReview(context);
        initiated.enable(context);

        checkDisableNotSupported(initiated, context);
        checkArchiveNotSupported(initiated, context);
    }

    private DefaultCustomSchemaStateExecutor createDefaultSchemaReviewExecutor() {
        DefaultCustomSchemaStateExecutor schemaReviewExecutor = new DefaultCustomSchemaStateExecutor();
        schemaReviewExecutor.init(SchemaVersionLifecycleStateMachine.newBuilder(),
                                  SchemaVersionLifecycleStates.REVIEWED.getId(),
                                  SchemaVersionLifecycleStates.CHANGES_REQUIRED.getId(),
                                  Collections.emptyMap());
        return schemaReviewExecutor;
    }

    @Test
    public void testEnabledState() throws Exception {

        InbuiltSchemaVersionLifecycleState enabled = SchemaVersionLifecycleStates.ENABLED;

        enabled.disable(context);
        enabled.archive(context);

        checkStartReviewNotSupported(enabled, context);
        checkEnableNotSupported(enabled, context);
        checkDeleteNotSupported(enabled, context);
    }


    @Test
    public void testDisabledState() throws Exception {

        InbuiltSchemaVersionLifecycleState disabled = SchemaVersionLifecycleStates.DISABLED;

        disabled.enable(context);
        disabled.archive(context);

        checkStartReviewNotSupported(disabled, context);
        checkDisableNotSupported(disabled, context);
        checkDeleteNotSupported(disabled, context);
    }

    @Test
    public void testArchivedState() throws Exception {

        InbuiltSchemaVersionLifecycleState archived = SchemaVersionLifecycleStates.ARCHIVED;

        checkStartReviewNotSupported(archived, context);
        checkEnableNotSupported(archived, context);
        checkDisableNotSupported(archived, context);
        checkArchiveNotSupported(archived, context);
    }

    @Test
    public void testDeletedState() throws Exception {

        InbuiltSchemaVersionLifecycleState deleted = SchemaVersionLifecycleStates.DELETED;

        checkStartReviewNotSupported(deleted, context);
        checkEnableNotSupported(deleted, context);
        checkDisableNotSupported(deleted, context);
        checkArchiveNotSupported(deleted, context);
        checkDeleteNotSupported(deleted, context);
    }

    private void checkArchiveNotSupported(InbuiltSchemaVersionLifecycleState state,
                                          SchemaVersionLifecycleContext context) throws SchemaNotFoundException {
        try {
            state.archive(context);
            Assert.fail(state.getName() + " should not lead to archive state");
        } catch (SchemaLifecycleException e) {
        }
    }

    private void checkDeleteNotSupported(InbuiltSchemaVersionLifecycleState state,
                                         SchemaVersionLifecycleContext context) throws SchemaNotFoundException {
        try {
            state.delete(context);
            Assert.fail(state.getName() + " should not lead to delete state");
        } catch (SchemaLifecycleException e) {
        }
    }

    private void checkDisableNotSupported(InbuiltSchemaVersionLifecycleState state,
                                          SchemaVersionLifecycleContext context) throws SchemaNotFoundException {
        try {
            state.disable(context);
            Assert.fail(state.getName() + " should not lead to disabled state");
        } catch (SchemaLifecycleException e) {
        }
    }

    private void checkEnableNotSupported(InbuiltSchemaVersionLifecycleState state,
                                         SchemaVersionLifecycleContext context) throws SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchNotFoundException {
        try {
            state.enable(context);
            Assert.fail(state.getName() + " should not lead to enable state");
        } catch (SchemaLifecycleException e) {
        }
    }

    private void checkStartReviewNotSupported(InbuiltSchemaVersionLifecycleState state,
                                              SchemaVersionLifecycleContext context) throws SchemaNotFoundException {
        try {
            state.startReview(context);
            Assert.fail(state.getName() + " should not lead to startReview state");
        } catch (SchemaLifecycleException e) {
        }
    }
}
