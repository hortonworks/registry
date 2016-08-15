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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidationStrategy;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AvroSchemaProvider implements SchemaProvider {

    public static final String TYPE = "avro";

    @Override
    public String getType() {
        return TYPE;
    }

    private enum CompatibilityStrategy {
        BACKWARD_COMPATIBILITY(new SchemaValidatorBuilder().canReadStrategy().validateAll()),
        FORWARD_COMPATIBILITY(new SchemaValidatorBuilder().canBeReadStrategy().validateAll()),
        BOTH_COMPATIBILITY(new SchemaValidatorBuilder().mutualReadStrategy().validateAll()),
        NONE_COMPATIBILITY(new SchemaValidatorBuilder().strategy(new SchemaValidationStrategy() {
            @Override
            public void validate(Schema toValidate, Schema existing) throws SchemaValidationException {
                throw new SchemaValidationException(toValidate, existing);
            }
        }).validateAll());

        private SchemaValidator schemaValidator;

        CompatibilityStrategy(SchemaValidator schemaValidator) {
            this.schemaValidator = schemaValidator;
        }

        public boolean validate(Schema toSchema, Schema existingSchema) {
            return validate(toSchema, Collections.singleton(existingSchema));
        }

        public boolean validate(Schema toSchema, Collection<Schema> existingSchemas) {
            try {
                schemaValidator.validate(toSchema, existingSchemas);
            } catch (SchemaValidationException e) {
                return false;
            }
            return true;
        }
    }

    @Override
    public boolean isCompatible(String toSchemaText, String existingSchemaText, Compatibility existingSchemaCompatibility) {
        return isCompatible(toSchemaText, Collections.singleton(existingSchemaText), existingSchemaCompatibility);
    }

    public boolean isCompatible(String toSchemaText, Collection<String> existingSchemaTexts, Compatibility existingSchemaCompatibility) {
        Schema toSchema = new Schema.Parser().parse(toSchemaText);
        Collection<Schema> existingSchemas = Collections2.transform(existingSchemaTexts, new Function<String, Schema>() {
            @Nullable
            @Override
            public Schema apply(@Nullable String input) {
                return new Schema.Parser().parse(input);
            }
        });

        CompatibilityStrategy compatibilityStrategy = null;
        switch(existingSchemaCompatibility) {
            case BACKWARD:
                compatibilityStrategy = CompatibilityStrategy.BACKWARD_COMPATIBILITY;
                break;
            case FORWARD:
                compatibilityStrategy = CompatibilityStrategy.FORWARD_COMPATIBILITY;
                break;
            case BOTH:
                compatibilityStrategy = CompatibilityStrategy.BOTH_COMPATIBILITY;
                break;
            case NONE:
                compatibilityStrategy = CompatibilityStrategy.NONE_COMPATIBILITY;
                break;
            default:
                throw new RuntimeException("Invalid schema compatibility");
        }

        return compatibilityStrategy.validate(toSchema, existingSchemas);
    }
}
