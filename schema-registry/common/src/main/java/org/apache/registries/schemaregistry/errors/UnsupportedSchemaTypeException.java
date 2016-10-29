package org.apache.registries.schemaregistry.errors;

/**
 * Indicates that schema type is invalid.
 */
public class UnsupportedSchemaTypeException extends Exception {

    public UnsupportedSchemaTypeException() {
        super();
    }

    public UnsupportedSchemaTypeException(String message) {
        super(message);
    }

    public UnsupportedSchemaTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportedSchemaTypeException(Throwable cause) {
        super(cause);
    }

    protected UnsupportedSchemaTypeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
