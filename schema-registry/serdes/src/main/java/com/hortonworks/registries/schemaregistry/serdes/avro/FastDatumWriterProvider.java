package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.rtbhouse.utils.avro.FastGenericDatumWriter;
import com.rtbhouse.utils.avro.FastSpecificDatumWriter;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificRecord;

public class FastDatumWriterProvider implements DatumWriterProvider {

    @Override
    public DatumWriter<Object> getDatumWriter(Schema schema, Object record) {
        if (record instanceof SpecificRecord) {
            return new FastSpecificDatumWriter<>(schema);
        } else {
            return new FastGenericDatumWriter<>(schema);
        }
    }
}
