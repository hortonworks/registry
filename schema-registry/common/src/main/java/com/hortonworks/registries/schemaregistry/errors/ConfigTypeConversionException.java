package com.hortonworks.registries.schemaregistry.errors;

public class ConfigTypeConversionException extends RuntimeException {

    public ConfigTypeConversionException(String message) { super(message); }

    public ConfigTypeConversionException(String message, Throwable cause) { super(message, cause); }
}
