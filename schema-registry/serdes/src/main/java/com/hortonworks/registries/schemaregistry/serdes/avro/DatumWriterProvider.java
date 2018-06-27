package com.hortonworks.registries.schemaregistry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;

public interface DatumWriterProvider {
    DatumWriter<Object> getDatumWriter(Schema schema, Object input);
}
