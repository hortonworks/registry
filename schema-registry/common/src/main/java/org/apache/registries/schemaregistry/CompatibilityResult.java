/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.registries.schemaregistry;

/**
 *
 */
public final class CompatibilityResult {
    public static final CompatibilityResult SUCCESS = new CompatibilityResult(true, null);

    private boolean compatible;
    private String errorMessage;

    private CompatibilityResult() {
    }

    private CompatibilityResult(boolean compatible, String errorMessage) {
        this.compatible = compatible;
        this.errorMessage = errorMessage;
    }

    public boolean isCompatible() {
        return compatible;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns {@link CompatibilityResult} instance with {@link CompatibilityResult#compatible} as false and {@link CompatibilityResult#errorMessage} as given {@code errorMessage}
     *
     * @param errorMessage
     */
    public static CompatibilityResult createIncompatibleResult(String errorMessage) {
        return new CompatibilityResult(false, errorMessage);
    }
}
