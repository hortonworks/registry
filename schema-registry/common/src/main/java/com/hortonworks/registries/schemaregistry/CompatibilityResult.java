/**
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
 **/
package com.hortonworks.registries.schemaregistry;

import java.io.Serializable;

/**
 *
 */
public final class CompatibilityResult implements Serializable {
    public static final CompatibilityResult SUCCESS = new CompatibilityResult(true, null);

    private boolean compatible;
    private String errorMessage;
    private String errorLocation;
    private String schema;

    private CompatibilityResult() {
    }

    private CompatibilityResult(boolean compatible, String schema) {
        this.compatible = compatible;
        this.schema = schema;
    }

    private CompatibilityResult(boolean compatible, String errorMessage, String errorLocation, String schema) {
        this.compatible = compatible;
        this.errorMessage = errorMessage;
        this.errorLocation = errorLocation;
        this.schema = schema;
    }

    public boolean isCompatible() {
        return compatible;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getSchema() {
        return schema;
    }

    public String getErrorLocation() {
        return errorLocation;
    }

    @Override
    public String toString() {
        return "CompatibilityResult{" +
                "compatible=" + compatible +
                ", errorMessage='" + errorMessage + '\'' +
                ", errorLocation='" + errorLocation + '\'' +
                ", schema='" + schema + '\'' +
                '}';
    }

    /**
     * Returns {@link CompatibilityResult} instance with {@link CompatibilityResult#compatible} as false and {@link CompatibilityResult#errorMessage} as given {@code errorMessage}
     *  @param errorMessage
     * @param errorLocation
     * @param schema
     */
    public static CompatibilityResult createIncompatibleResult(String errorMessage, String errorLocation, String schema) {
        return new CompatibilityResult(false, errorMessage, errorLocation, schema);
    }

    public static CompatibilityResult createCompatibleResult(String schema) {
        return new CompatibilityResult(true, schema);
    }
}
