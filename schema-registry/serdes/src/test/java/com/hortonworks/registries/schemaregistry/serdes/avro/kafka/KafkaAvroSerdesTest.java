package com.hortonworks.registries.schemaregistry.serdes.avro.kafka;

import java.util.HashMap;
import java.util.Map;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.MockSchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.TestRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class KafkaAvroSerdesTest {

    private ISchemaRegistryClient schemaRegistryClient;

    @Before
    public void setup(){
        schemaRegistryClient = new MockSchemaRegistryClient();
    }

    @Test
    public void testGenericSerializedSpecificDeserialized() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);
        
        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);

        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"com.hortonworks.registries.schemaregistry.serdes.avro\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}");

        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();


        byte[] bytes = kafkaAvroSerializer.serialize("topic" , record);
        
        Object o = kafkaAvroDeserializer.deserialize("topic", bytes);

        Assert.assertEquals(o.getClass(), TestRecord.class);
        
        TestRecord result = (TestRecord) o; 
        
        Assert.assertEquals(record.get("field1"), result.getField1());
        Assert.assertEquals(record.get("field2"), result.getField2());


    }

    @Test
    public void testSpecificSerializedSpecificDeserialized() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        
        TestRecord record = new TestRecord();
        record.setField1("some value");
        record.setField1("some other value");
        
        byte[] bytes = kafkaAvroSerializer.serialize("topic" , record);

        Object o = kafkaAvroDeserializer.deserialize("topic", bytes);

        Assert.assertEquals(o.getClass(), TestRecord.class);

        TestRecord result = (TestRecord) o;

        Assert.assertEquals(record.getField1(), result.getField1());
        Assert.assertEquals(record.getField2(), result.getField2());

    }


    @Test
    public void testSpecificSerializedGenericDeserialized() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, false);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);

        TestRecord record = new TestRecord();
        record.setField1("some value");
        record.setField1("some other value");

        byte[] bytes = kafkaAvroSerializer.serialize("topic" , record);

        Object o = kafkaAvroDeserializer.deserialize("topic", bytes);

        Assert.assertEquals(o.getClass(), GenericData.Record.class);

        GenericRecord result = (GenericRecord) o;

        Assert.assertEquals(record.getField1(), result.get("field1"));
        Assert.assertEquals(record.getField2(), result.get("field2"));

    }

    @Test
    public void testGenericSerializedGenericDeserialized() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, false);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);

        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"com.hortonworks.registries.schemaregistry.serdes.avro\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}");

        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();

        byte[] bytes = kafkaAvroSerializer.serialize("topic" , record);

        Object o = kafkaAvroDeserializer.deserialize("topic", bytes);

        Assert.assertEquals(o.getClass(), GenericData.Record.class);

        GenericRecord result = (GenericRecord) o;

        Assert.assertEquals(record.get("field1"), result.get("field1"));
        Assert.assertEquals(record.get("field2"), result.get("field2"));

    }
}
