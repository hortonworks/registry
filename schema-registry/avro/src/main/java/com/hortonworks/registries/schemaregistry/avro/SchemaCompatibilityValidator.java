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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaProvider;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidationStrategy;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class SchemaCompatibilityValidator {

    private static final Map<SchemaProvider.Compatibility, SchemaValidator> COMPATIBILITY_VALIDATORS;

    static {
        Map<SchemaProvider.Compatibility, SchemaValidator> validators = new HashMap<>();
        validators.put(SchemaProvider.Compatibility.BACKWARD, new SchemaValidatorBuilder().canReadStrategy().validateAll());
        validators.put(SchemaProvider.Compatibility.FORWARD, new SchemaValidatorBuilder().canBeReadStrategy().validateAll());
        validators.put(SchemaProvider.Compatibility.BOTH, new SchemaValidatorBuilder().mutualReadStrategy().validateAll());
        validators.put(SchemaProvider.Compatibility.NONE, new SchemaValidatorBuilder().strategy(new SchemaValidationStrategy() {
            @Override
            public void validate(Schema toValidate, Schema existing) throws SchemaValidationException {
                throw new SchemaValidationException(toValidate, existing);
            }
        }).validateAll());
        COMPATIBILITY_VALIDATORS = Collections.unmodifiableMap(validators);
    }

    static SchemaValidator of(SchemaProvider.Compatibility compatibility) {
        return COMPATIBILITY_VALIDATORS.get(compatibility);
    }
}
