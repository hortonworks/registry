/**
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.hortonworks.registries.serdes;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Device extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Device\",\"namespace\":\"com.hortonworks.registries.serdes\"," +
                    "\"fields\":[{\"name\":\"xid\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}," +
                    "{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}");
    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA;
    }
    @Deprecated public long xid;
    @Deprecated public CharSequence name;
    @Deprecated public int version;
    @Deprecated public long timestamp;

    /**
     * Default constructor.  Note that this does not initialize fields
     * to their default values from the schema.  If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public Device() { }

    /**
     * All-args constructor.
     */
    public Device(Long xid, CharSequence name, Integer version, Long timestamp) {
        this.xid = xid;
        this.name = name;
        this.version = version;
        this.timestamp = timestamp;
    }

    public org.apache.avro.Schema getSchema() {
        return SCHEMA; 
    }
    // Used by DatumWriter.  Applications should not call.
    public Object get(int field) {
        switch (field) {
            case 0: return xid;
            case 1: return name;
            case 2: return version;
            case 3: return timestamp;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }
    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value = "unchecked")
    public void put(int field, Object value) {
        switch (field) {
            case 0: xid = (Long) value;
                break;
            case 1: name = (CharSequence) value;
                break;
            case 2: version = (Integer) value;
                break;
            case 3: timestamp = (Long) value;
                break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'xid' field.
     */
    public Long getXid() {
        return xid;
    }

    /**
     * Sets the value of the 'xid' field.
     * @param value the value to set.
     */
    public void setXid(Long value) {
        this.xid = value;
    }

    /**
     * Gets the value of the 'name' field.
     */
    public CharSequence getName() {
        return name;
    }

    /**
     * Sets the value of the 'name' field.
     * @param value the value to set.
     */
    public void setName(CharSequence value) {
        this.name = value;
    }

    /**
     * Gets the value of the 'version' field.
     */
    public Integer getVersion() {
        return version;
    }

    /**
     * Sets the value of the 'version' field.
     * @param value the value to set.
     */
    public void setVersion(Integer value) {
        this.version = value;
    }

    /**
     * Gets the value of the 'timestamp' field.
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the value of the 'timestamp' field.
     * @param value the value to set.
     */
    public void setTimestamp(Long value) {
        this.timestamp = value;
    }

    /** Creates a new Device RecordBuilder */
    public static Device.Builder newBuilder() {
        return new Device.Builder();
    }

    /** Creates a new Device RecordBuilder by copying an existing Builder */
    public static Device.Builder newBuilder(Device.Builder other) {
        return new Device.Builder(other);
    }

    /** Creates a new Device RecordBuilder by copying an existing Device instance */
    public static Device.Builder newBuilder(Device other) {
        return new Device.Builder(other);
    }

    /**
     * RecordBuilder for Device instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Device>
            implements org.apache.avro.data.RecordBuilder<Device> {

        private long xid;
        private CharSequence name;
        private int version;
        private long timestamp;

        /** Creates a new Builder */
        private Builder() {
            super(Device.SCHEMA);
        }

        /** Creates a Builder by copying an existing Builder */
        private Builder(Device.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.xid)) {
                this.xid = data().deepCopy(fields()[0].schema(), other.xid);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.name)) {
                this.name = data().deepCopy(fields()[1].schema(), other.name);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.version)) {
                this.version = data().deepCopy(fields()[2].schema(), other.version);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.timestamp)) {
                this.timestamp = data().deepCopy(fields()[3].schema(), other.timestamp);
                fieldSetFlags()[3] = true;
            }
        }

        /** Creates a Builder by copying an existing Device instance */
        private Builder(Device other) {
            super(Device.SCHEMA);
            if (isValidValue(fields()[0], other.xid)) {
                this.xid = data().deepCopy(fields()[0].schema(), other.xid);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.name)) {
                this.name = data().deepCopy(fields()[1].schema(), other.name);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.version)) {
                this.version = data().deepCopy(fields()[2].schema(), other.version);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.timestamp)) {
                this.timestamp = data().deepCopy(fields()[3].schema(), other.timestamp);
                fieldSetFlags()[3] = true;
            }
        }

        /** Gets the value of the 'xid' field */
        public Long getXid() {
            return xid;
        }

        /** Sets the value of the 'xid' field */
        public Device.Builder setXid(long value) {
            validate(fields()[0], value);
            this.xid = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /** Checks whether the 'xid' field has been set */
        public boolean hasXid() {
            return fieldSetFlags()[0];
        }

        /** Clears the value of the 'xid' field */
        public Device.Builder clearXid() {
            fieldSetFlags()[0] = false;
            return this;
        }

        /** Gets the value of the 'name' field */
        public CharSequence getName() {
            return name;
        }

        /** Sets the value of the 'name' field */
        public Device.Builder setName(CharSequence value) {
            validate(fields()[1], value);
            this.name = value;
            fieldSetFlags()[1] = true;
            return this;
        }

        /** Checks whether the 'name' field has been set */
        public boolean hasName() {
            return fieldSetFlags()[1];
        }

        /** Clears the value of the 'name' field */
        public Device.Builder clearName() {
            name = null;
            fieldSetFlags()[1] = false;
            return this;
        }

        /** Gets the value of the 'version' field */
        public Integer getVersion() {
            return version;
        }

        /** Sets the value of the 'version' field */
        public Device.Builder setVersion(int value) {
            validate(fields()[2], value);
            this.version = value;
            fieldSetFlags()[2] = true;
            return this;
        }

        /** Checks whether the 'version' field has been set */
        public boolean hasVersion() {
            return fieldSetFlags()[2];
        }

        /** Clears the value of the 'version' field */
        public Device.Builder clearVersion() {
            fieldSetFlags()[2] = false;
            return this;
        }

        /** Gets the value of the 'timestamp' field */
        public Long getTimestamp() {
            return timestamp;
        }

        /** Sets the value of the 'timestamp' field */
        public Device.Builder setTimestamp(long value) {
            validate(fields()[3], value);
            this.timestamp = value;
            fieldSetFlags()[3] = true;
            return this;
        }

        /** Checks whether the 'timestamp' field has been set */
        public boolean hasTimestamp() {
            return fieldSetFlags()[3];
        }

        /** Clears the value of the 'timestamp' field */
        public Device.Builder clearTimestamp() {
            fieldSetFlags()[3] = false;
            return this;
        }

        @Override
        public Device build() {
            try {
                Device record = new Device();
                record.xid = fieldSetFlags()[0] ? this.xid : (Long) defaultValue(fields()[0]);
                record.name = fieldSetFlags()[1] ? this.name : (CharSequence) defaultValue(fields()[1]);
                record.version = fieldSetFlags()[2] ? this.version : (Integer) defaultValue(fields()[2]);
                record.timestamp = fieldSetFlags()[3] ? this.timestamp : (Long) defaultValue(fields()[3]);
                return record;
            } catch (Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }
}
