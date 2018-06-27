package com.hortonworks.registries.schemaregistry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;

public interface DatumReaderProvider {

    DatumReader getDatumReader(Schema writerSchema, Schema readerSchema);
}
