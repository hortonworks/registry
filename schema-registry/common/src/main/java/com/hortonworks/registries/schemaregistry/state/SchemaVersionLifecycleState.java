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

import java.util.List;

/**
 * Schema version life cycle state to be defined.
 */
public interface SchemaVersionLifecycleState {

    /**
     * @return This state's identifier. Identifiers &lt;= 32 are reserved and 33 to 127 can be used for any custom
     * states.
     */
    Byte id();

    /**
     * @return name of this state
     */
    String name();

    /**
     * @return description about this state
     */
    String description();

    /**
     * @return List of states that can lead from this state.
     */
    default List<SchemaVersionLifecycleState> nextStates() {
        throw new UnsupportedOperationException();
    }
}
