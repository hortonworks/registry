package com.hortonworks.registries.schemaregistry.serde.pull;

/**
 *
 */
public class EndFieldContext<F> implements PullEventContext<F> {

    private final FieldValue<F> fieldValue;

    public EndFieldContext(FieldValue<F> fieldValue) {
        this.fieldValue = fieldValue;
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
        return false;
    }

    @Override
    public boolean endField() {
        return true;
    }

    @Override
    public F currentField() {
        return fieldValue.field();
    }

    @Override
    public FieldValue<F> fieldValue() {
        return fieldValue;
    }
}
