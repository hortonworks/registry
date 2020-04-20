package com.hortonworks.registries.schemaregistry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

public class DefaultDatumWriterProvider implements DatumWriterProvider {

    @Override
    public DatumWriter<Object> getDatumWriter(Schema schema, Object record) {
        if (record instanceof SpecificRecord) {
            return new SpecificDatumWriter<>(schema);
        } else {
            return new GenericDatumWriter<>(schema);
        }
    }
}
