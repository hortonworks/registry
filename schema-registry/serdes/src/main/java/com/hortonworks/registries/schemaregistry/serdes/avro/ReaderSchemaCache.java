package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

public class ReaderSchemaCache {

    private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

    protected Schema getReaderSchema(Schema writerSchema) {
        Schema readerSchema = this.readerSchemaCache.get(writerSchema.getFullName());
        if (readerSchema == null) {
            Class readerClass = SpecificData.get().getClass(writerSchema);
            if (readerClass == null) {
                throw new AvroException("Could not find class " + writerSchema.getFullName() + " specified in writer\'s schema whilst finding reader\'s schema for a SpecificRecord.");
            }
            try {
                readerSchema = ((SpecificRecord) readerClass.newInstance()).getSchema();
            } catch (InstantiationException e) {
                throw new AvroException(writerSchema.getFullName() + " specified by the " + "writers schema could not be instantiated to find the readers schema.");
            } catch (IllegalAccessException e) {
                throw new AvroException(writerSchema.getFullName() + " specified by the " + "writers schema is not allowed to be instantiated to find the readers schema.");
            }

            this.readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
        }

        return readerSchema;
    }
}
