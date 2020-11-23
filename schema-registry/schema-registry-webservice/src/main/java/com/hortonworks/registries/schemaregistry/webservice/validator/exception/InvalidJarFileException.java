package com.hortonworks.registries.schemaregistry.webservice.validator.exception;

public class InvalidJarFileException extends Exception {
    /**
     *  
     */
    private static final long serialVersionUID = -7688628446475079619L;

    public InvalidJarFileException(String message) {
        super(message);
    }

    public InvalidJarFileException(String message, Throwable cause) {
        super(message, cause);
    }
}