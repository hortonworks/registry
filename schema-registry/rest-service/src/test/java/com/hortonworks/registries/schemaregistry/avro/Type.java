package com.hortonworks.registries.schemaregistry.avro;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum Type {

    AVRO("avro"),
    XSD("xsd");

    private final static Map<String, Type> map = Collections.unmodifiableMap(
            new HashMap<String, Type>() {{
                for (Type type : Type.values()) put(type.value, type);
            }}
    );

    /**
     * Get the enum type using the corresponding value.
     *
     * @param value - value of the enum.
     * @return EventSchema.Type corresponding to the passed value.
     */
    public static Type getType(String value) {
        return map.get(value.toLowerCase());
    }

    private final String value;

    public String value() {
        return this.value;
    }

    Type(String value) {
        this.value = value;
    }
}
