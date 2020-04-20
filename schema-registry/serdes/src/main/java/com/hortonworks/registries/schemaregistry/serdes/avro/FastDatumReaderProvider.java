package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.rtbhouse.utils.avro.FastGenericDatumReader;
import com.rtbhouse.utils.avro.FastSpecificDatumReader;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;

public class FastDatumReaderProvider implements DatumReaderProvider {

    private final ReaderSchemaCache readerSchemaCache = new ReaderSchemaCache();

    private final boolean useSpecificAvroReader;

    public FastDatumReaderProvider(boolean useSpecificAvroReader) {
        this.useSpecificAvroReader = useSpecificAvroReader;
    }

    @Override
    public DatumReader getDatumReader(Schema writerSchema, Schema readerSchema) {
        if (useSpecificAvroReader) {
            if (readerSchema == null) {
                readerSchema = readerSchemaCache.getReaderSchema(writerSchema);
            }

            return new FastSpecificDatumReader(writerSchema, readerSchema);
        } else {
            return readerSchema == null ? new FastGenericDatumReader(writerSchema) : new FastGenericDatumReader(writerSchema, readerSchema);
        }
    }
}
