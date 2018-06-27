package com.hortonworks.registries.schemaregistry.serdes.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class DefaultDatumReaderProvider implements DatumReaderProvider {

    private final ReaderSchemaCache readerSchemaCache = new ReaderSchemaCache();

    private final boolean useSpecificAvroReader;

    public DefaultDatumReaderProvider(boolean useSpecificAvroReader) {
        this.useSpecificAvroReader = useSpecificAvroReader;
    }

    @Override
    public DatumReader getDatumReader(Schema writerSchema, Schema readerSchema) {
        if (useSpecificAvroReader) {
            if (readerSchema == null) {
                readerSchema = readerSchemaCache.getReaderSchema(writerSchema);
            }

            return new SpecificDatumReader(writerSchema, readerSchema);
        } else {
            return readerSchema == null ? new GenericDatumReader(writerSchema) : new GenericDatumReader(writerSchema, readerSchema);
        }
    }
}
