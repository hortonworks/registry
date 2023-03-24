/**
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Utils class for Avro related functionality.
 */
public final class AvroUtils {

    private static final Map<Schema.Type, Schema> PRIMITIVE_SCHEMAS;
    private static final GenericData GENERIC_DATA_WITH_LOGICAL_TYPE_CONVERSIONS;

    static {
        Map<Schema.Type, Schema> map = new HashMap<>();
        Schema.Type[] types = {Schema.Type.NULL, Schema.Type.BYTES, Schema.Type.INT, Schema.Type.FLOAT,
                Schema.Type.DOUBLE, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.BOOLEAN};
        for (Schema.Type type : types) {
            map.put(type, Schema.create(type));
        }
        PRIMITIVE_SCHEMAS = Collections.unmodifiableMap(map);

        GENERIC_DATA_WITH_LOGICAL_TYPE_CONVERSIONS = new GenericData();
        Arrays.asList(
                new Conversions.DecimalConversion(),
                new Conversions.UUIDConversion(),
                new TimeConversions.DateConversion(),
                new TimeConversions.TimeMillisConversion(),
                new TimeConversions.TimeMicrosConversion(),
                new TimeConversions.LocalTimestampMillisConversion(),
                new TimeConversions.LocalTimestampMicrosConversion(),
                new TimeConversions.TimestampMillisConversion(),
                new TimeConversions.TimestampMicrosConversion()
        ).forEach(GENERIC_DATA_WITH_LOGICAL_TYPE_CONVERSIONS::addLogicalTypeConversion);
    }

    private AvroUtils() {
    }

    public static Schema getSchemaForPrimitives(Object input) {
        Schema.Type type;
        if (input == null) {
            type = Schema.Type.NULL;
        } else if (input instanceof byte[]) {
            type = Schema.Type.BYTES;
        } else if (input instanceof Integer || input instanceof Short || input instanceof Byte) {
            type = Schema.Type.INT;
        } else if (input instanceof Float) {
            type = Schema.Type.FLOAT;
        } else if (input instanceof Double) {
            type = Schema.Type.DOUBLE;
        } else if (input instanceof Long) {
            type = Schema.Type.LONG;
        } else if (input instanceof String) {
            type = Schema.Type.STRING;
        } else if (input instanceof Boolean) {
            type = Schema.Type.BOOLEAN;
        } else {
            throw new AvroException("input type: " + input.getClass() + " is not supported");
        }

        return PRIMITIVE_SCHEMAS.get(type);

    }

    public static Schema computeSchema(Object input) {
        Schema schema;
        if (input instanceof GenericContainer) {
            schema = ((GenericContainer) input).getSchema();
        } else {
            schema = AvroUtils.getSchemaForPrimitives(input);
        }
        return schema;
    }

    public static GenericData getGenericData(boolean logicalTypeConversionEnabled) {
        if (logicalTypeConversionEnabled) {
            return GENERIC_DATA_WITH_LOGICAL_TYPE_CONVERSIONS;
        } else {
            return GenericData.get();
        }
    }

}
