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

package com.hortonworks.registries.schemaregistry.state;

/**
 *  Interface serves as a template for users to register a listener whenever a state transition is triggered
 */
public interface SchemaVersionLifecycleStateTransitionListener {

    /**
     *  This method is called before a schema version transitions to the target state
     * @param context  contains contextual information and services required for transition from one state to other state.
     */
    void preStateTransition(final SchemaVersionLifecycleContext context);

    /**
     *  This method is called after a schema version transitions to the target state
     * @param context contains contextual information and services required for transition from one state to other state.
     */
    void postStateTransition(final SchemaVersionLifecycleContext context);
}
