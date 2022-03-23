/**
 * Copyright 2016-2022 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;

/**
 * Schema compatibility settings. When the schema creator does not provide
 * the compatibility value for the new schema then these default values
 * will be applied.
 */
public class CompatibilityConfig {

    @JsonProperty
    private String avroCompatibility = SchemaCompatibility.DEFAULT_COMPATIBILITY.name();
    @JsonProperty
    private String jsonCompatibility = SchemaCompatibility.NONE.name();
    @JsonProperty
    private String validationLevel = SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL.name();

    public CompatibilityConfig() { }

    public String getAvroCompatibility() {
        return avroCompatibility;
    }

    public void setAvroCompatibility(String avroCompatibility) {
        this.avroCompatibility = avroCompatibility;
    }

    public String getJsonCompatibility() {
        return jsonCompatibility;
    }

    public void setJsonCompatibility(String jsonCompatibility) {
        this.jsonCompatibility = jsonCompatibility;
    }

    public String getValidationLevel() {
        return validationLevel;
    }

    public void setValidationLevel(String validationLevel) {
        this.validationLevel = validationLevel;
    }
}
