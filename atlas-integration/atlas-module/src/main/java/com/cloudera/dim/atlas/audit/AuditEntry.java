/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.cloudera.dim.atlas.audit;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class AuditEntry {

    private String methodName;
    private List<String> paramTypes = new ArrayList<>();
    private List<String> paramValues = new ArrayList<>();

    public AuditEntry() { }

    @JsonProperty
    public String getMethodName() {
        return methodName;
    }

    @JsonProperty
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @JsonProperty
    public List<String> getParamTypes() {
        return paramTypes;
    }

    @JsonProperty
    public void setParamTypes(List<String> paramTypes) {
        this.paramTypes = paramTypes;
    }

    @JsonProperty
    public List<String> getParamValues() {
        return paramValues;
    }

    @JsonProperty
    public void setParamValues(List<String> paramValues) {
        this.paramValues = paramValues;
    }
}
