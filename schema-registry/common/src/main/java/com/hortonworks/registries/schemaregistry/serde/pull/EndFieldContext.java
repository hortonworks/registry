/**
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.serde.pull;

/**
 *
 */
public class EndFieldContext<F> implements PullEventContext<F> {

    private final FieldValue<F> fieldValue;

    public EndFieldContext(FieldValue<F> fieldValue) {
        this.fieldValue = fieldValue;
    }

    @Override
    public boolean startRecord() {
        return false;
    }

    @Override
    public boolean endRecord() {
        return false;
    }

    @Override
    public boolean startField() {
        return false;
    }

    @Override
    public boolean endField() {
        return true;
    }

    @Override
    public F currentField() {
        return fieldValue.field();
    }

    @Override
    public FieldValue<F> fieldValue() {
        return fieldValue;
    }
}
