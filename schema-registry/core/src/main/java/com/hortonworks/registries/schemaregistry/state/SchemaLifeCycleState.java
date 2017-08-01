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

/**
 *
 */
public interface SchemaLifeCycleState {

    /**
     * @return This state's identifier
     */
    byte id();

    /**
     * @return name of this state
     */
    String name();

    /**
     * @return description about this state
     */
    String description();

    public default void startReview(SchemaLifeCycleContext schemaLifeCycleContext) throws SchemaLifeCycleException {
        throw new UnsupportedOperationException(" This operation is not supported for this instance: " + this);
    };

    public default void enable(SchemaLifeCycleContext schemaLifeCycleContext) throws SchemaLifeCycleException{
        throw new UnsupportedOperationException(" This operation is not supported for this instance: " + this);
    }

    public default void disable(SchemaLifeCycleContext schemaLifeCycleContext) throws SchemaLifeCycleException{
        throw new UnsupportedOperationException(" This operation is not supported for this instance: " + this);

    }

    public default void archive(SchemaLifeCycleContext schemaLifeCycleContext) throws SchemaLifeCycleException{
        throw new UnsupportedOperationException(" This operation is not supported for this instance: " + this);
    }

    public default void delete(SchemaLifeCycleContext schemaLifeCycleContext) throws SchemaLifeCycleException{
        throw new UnsupportedOperationException(" This operation is not supported for this instance: " + this);
    }

}
