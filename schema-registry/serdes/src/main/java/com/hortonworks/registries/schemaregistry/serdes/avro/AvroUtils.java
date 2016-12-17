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
package com.hortonworks.registries.schemaregistry.serdes.avro;

import org.apache.avro.Schema;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Utils class for Avro related functionality.
 */
public final class AvroUtils {
    public static final Charset UTF_8 = Charset.forName("UTF-8");
    public static final byte GENERIC_RECORD = 0x0;
    public static final byte SPECIFIC_RECORD = 0x1;

    private static final Map<Schema.Type, Schema> PRIMITIVE_SCHEMAS;


    static {
        Map<Schema.Type, Schema> map = new HashMap<>();
        Schema.Type[] types = {Schema.Type.NULL, Schema.Type.BYTES, Schema.Type.INT, Schema.Type.FLOAT,
                Schema.Type.DOUBLE, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.BOOLEAN};
        for (Schema.Type type : types) {
            map.put(type, Schema.create(type));
        }
        PRIMITIVE_SCHEMAS = Collections.unmodifiableMap(map);
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
            throw new RuntimeException("input type: " + input.getClass() + " is not supported");
        }

        return PRIMITIVE_SCHEMAS.get(type);

    }
}
