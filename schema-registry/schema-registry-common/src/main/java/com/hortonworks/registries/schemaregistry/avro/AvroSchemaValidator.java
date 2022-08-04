/**
 * Copyright 2017-2021 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaValidator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityResult;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Evaluate the compatibility between a reader schema and a writer schema.
 * A reader and a writer schema are declared compatible if all datum instances of the writer
 * schema can be successfully decoded using the specified reader schema.
 */
public final class AvroSchemaValidator implements SchemaValidator<Schema> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaValidator.class);

    private static final Map<SchemaCompatibility, SchemaCompatibilityValidator<Schema>> COMPATIBILITY_VALIDATORS;

    static {
        AvroSchemaValidator avroSchemaValidator = new AvroSchemaValidator();
        Map<SchemaCompatibility, SchemaCompatibilityValidator<Schema>> validators = new HashMap<>();
        validators.put(SchemaCompatibility.BACKWARD, new BackwardCompatibilityValidator<>(avroSchemaValidator));
        validators.put(SchemaCompatibility.FORWARD, new ForwardCompatibilityValidator<>(avroSchemaValidator));
        validators.put(SchemaCompatibility.BOTH, new BothCompatibilityValidator<>(avroSchemaValidator));
        validators.put(SchemaCompatibility.NONE, new NoneCompatibilityValidator<>());

        COMPATIBILITY_VALIDATORS = Collections.unmodifiableMap(validators);
    }

    static SchemaCompatibilityValidator<Schema> of(SchemaCompatibility compatibility) {
        return COMPATIBILITY_VALIDATORS.get(compatibility);
    }

    /**
     * Utility class cannot be instantiated.
     */
    private AvroSchemaValidator() {
    }

    public CompatibilityResult validate(Schema readerSchema, Schema writerSchema) {
        SchemaPairCompatibility schemaPairCompatibility = checkReaderWriterCompatibility(readerSchema, writerSchema);
        SchemaCompatibilityResult result = schemaPairCompatibility.getResult();
        if (result.getCompatibility() == SchemaCompatibilityType.COMPATIBLE) {
            return CompatibilityResult.createCompatibleResult(writerSchema.toString());
        } else {
            Incompatibility firstIncompatibility = result.getIncompatibilities().iterator().next();
            return CompatibilityResult.createIncompatibleResult(
                    firstIncompatibility.getMessage(),
                    firstIncompatibility.getLocation(),
                    writerSchema.toString()
            );
        }
    }

    /**
     * Validates that the provided reader schema can be used to decode avro data
     * written with the provided writer schema.
     *
     * @param reader schema to check.
     * @param writer schema to check.
     *
     * @return a result object identifying any compatibility errors.
     */
    public static SchemaPairCompatibility checkReaderWriterCompatibility(final Schema reader, final Schema writer) {
        SchemaPairCompatibility compatibility = org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
        if (compatibility.getType() == SchemaCompatibilityType.INCOMPATIBLE) {
            List<Incompatibility> incompatibilities = compatibility.getResult().getIncompatibilities();
            if (incompatibilities.stream().allMatch(AvroSchemaValidator::nullDefaultValueIncompatibility)) {
                compatibility = new SchemaPairCompatibility(SchemaCompatibilityResult.compatible(),
                        compatibility.getReader(),
                        compatibility.getWriter(),
                        compatibility.getDescription()
                );
            }
        }
        return compatibility;
    }

    private static boolean nullDefaultValueIncompatibility(Incompatibility incompatibility) {
        if (incompatibility.getType() == SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE) {
            String location = incompatibility.getLocation();
            int fieldIndex = Integer.parseInt(location.substring(location.lastIndexOf('/') + 1));
            Schema.Field field = incompatibility.getReaderFragment().getFields().get(fieldIndex);
            return isUnionWithFirstTypeAsNull(field.schema());
        }
        return false;
    }

    private static boolean isUnionWithFirstTypeAsNull(Schema readerFieldSchema) {
        return readerFieldSchema.getType() == Schema.Type.UNION
                && readerFieldSchema.getTypes().get(0).getType() == Schema.Type.NULL;
    }

    // -----------------------------------------------------------------------------------------------

    /**
     * Tests the equality of two Avro named schemas.
     * <p>
     * Matching includes reader name aliases.
     * </p>
     *
     * @param reader Named reader schema.
     * @param writer Named writer schema.
     *
     * @return whether the names of the named schemas match or not.
     */
    public static boolean schemaNameEquals(final Schema reader, final Schema writer) {
        final String writerFullName = writer.getFullName();
        if (objectsEqual(reader.getFullName(), writerFullName)) {
            return true;
        }
        // Apply reader aliases:
        if (reader.getAliases().contains(writerFullName)) {
            return true;
        }
        return false;
    }

    /**
     * Identifies the writer field that corresponds to the specified reader field.
     * <p>
     * Matching includes reader name aliases.
     * </p>
     *
     * @param writerSchema Schema of the record where to look for the writer
     *                     field.
     * @param readerField  Reader field to identify the corresponding writer field
     *                     of.
     *
     * @return the writer field, if any does correspond, or None.
     */
    public static Schema.Field lookupWriterField(final Schema writerSchema, final Schema.Field readerField) {
        assert (writerSchema.getType() == Schema.Type.RECORD);
        final List<Schema.Field> writerFields = new ArrayList<>();
        final Schema.Field direct = writerSchema.getField(readerField.name());
        if (direct != null) {
            writerFields.add(direct);
        }
        for (final String readerFieldAliasName : readerField.aliases()) {
            final Schema.Field writerField = writerSchema.getField(readerFieldAliasName);
            if (writerField != null) {
                writerFields.add(writerField);
            }
        }
        switch (writerFields.size()) {
            case 0:
                return null;
            case 1:
                return writerFields.get(0);
            default: {
                throw new AvroRuntimeException(String.format("Reader record field %s matches multiple fields in writer " +
                                                                     "record schema %s",
                                                             readerField,
                                                             writerSchema));
            }
        }
    }

    // -----------------------------------------------------------------------------------------------

    /**
     * Borrowed from Guava's Objects.equal(a, b)
     */
    private static boolean objectsEqual(Object obj1, Object obj2) {
        return Objects.equals(obj1, obj2);
    }

}
