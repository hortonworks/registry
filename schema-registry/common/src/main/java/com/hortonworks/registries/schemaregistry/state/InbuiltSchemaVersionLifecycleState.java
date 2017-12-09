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

import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;

/**
 *
 */
public interface InbuiltSchemaVersionLifecycleState extends SchemaVersionLifecycleState {

    default void startReview(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    default void enable(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    default void disable(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);

    }

    default void archive(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    default void delete(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaLifecycleException, SchemaNotFoundException {
        throw new SchemaLifecycleException(" This operation is not supported for this instance: " + this);
    }

    Collection<Pair<SchemaVersionLifecycleStateTransition, SchemaVersionLifecycleStateAction>> getTransitionActions();

}
