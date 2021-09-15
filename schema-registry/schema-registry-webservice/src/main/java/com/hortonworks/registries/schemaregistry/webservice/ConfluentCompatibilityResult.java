/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.webservice;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ConfluentCompatibilityResult implements Serializable {
    private static final long serialVersionUID = -8261546721396546755L;

    @JsonProperty("is_compatible")
    private boolean compatible;
    private String errorMessage;
    private String errorLocation;
    private String schema;

    private ConfluentCompatibilityResult() {
    }

    ConfluentCompatibilityResult(CompatibilityResult compatibilityResult) {
        this.compatible = compatibilityResult.isCompatible();
        this.schema = compatibilityResult.getSchema();
        this.errorMessage = compatibilityResult.getErrorMessage();
        this.errorLocation = compatibilityResult.getErrorLocation();
        this.schema = compatibilityResult.getSchema();
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
        return "ConfluentCompatibilityResult{" +
                "is_compatible=" + compatible +
                ", errorMessage='" + errorMessage + '\'' +
                ", errorLocation='" + errorLocation + '\'' +
                ", schema='" + schema + '\'' +
                '}';
    }
}
