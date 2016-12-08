package com.hortonworks.registries.schemaregistry.serde.pull;

/**
 *
 */
public class StartFieldContext<F> implements PullEventContext<F> {

    private final F currentField;

    public StartFieldContext(F currentField) {
        this.currentField = currentField;
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
        return true;
    }

    @Override
    public boolean endField() {
        return false;
    }

    @Override
    public F currentField() {
        return currentField;
    }

    @Override
    public FieldValue<F> fieldValue() {
        return null;
    }
}
