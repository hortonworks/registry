package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;

public class AbstractSnapshotDeserializerTest {

    @Test
    public void getBooleanValue_CanReadStringValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", "true");
        
        //when
        Boolean actual = underTest.getBooleanValue(config, "some key", false);
        
        //then
        Assert.assertThat(actual, is(true));
    }

    @Test
    public void getBooleanValue_CanReadBooleanValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Boolean> config = new HashMap<>();
        config.put("some key", true);

        //when
        Boolean actual = underTest.getBooleanValue(config, "some key", false);

        //then
        Assert.assertThat(actual, is(true));
    }

    @Test
    public void getBooleanValue_CanReadDefaultValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Boolean> config = new HashMap<>();
        config.put("some key", null);

        //when
        Boolean actual = underTest.getBooleanValue(config, "some key", false);

        //then
        Assert.assertThat(actual, is(false));
    }

    @Test
    public void getIntegerValue_CanReadIntegerValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Integer> config = new HashMap<>();
        config.put("some key", 4);

        //when
        Integer actual = underTest.getIntegerValue(config, "some key", 99);

        //then
        Assert.assertThat(actual, is(4));
    }

    @Test
    public void getIntegerValue_CanReadStringValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", "4");

        //when
        Integer actual = underTest.getIntegerValue(config, "some key", 99);

        //then
        Assert.assertThat(actual, is(4));
    }

    @Test
    public void getIntegerValue_CanReadDefaultValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Integer> config = new HashMap<>();
        config.put("some key", null);

        //when
        Integer actual = underTest.getIntegerValue(config, "some key", 99);

        //then
        Assert.assertThat(actual, is(99));
    }

    @Test
    public void getLongValue_CanReadLongValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, Long> config = new HashMap<>();
        config.put("some key", 673L);

        //when
        Long actual = underTest.getLongValue(config, "some key", 99L);

        //then
        Assert.assertThat(actual, is(673L));
    }

    @Test
    public void getLongValue_CanReadStringValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", "673");

        //when
        Long actual = underTest.getLongValue(config, "some key", 99L);

        //then
        Assert.assertThat(actual, is(673L));
    }

    @Test
    public void getLongValue_CanReadDefaultValue() {
        //given
        SnapshotDeserializer underTest = new SnapshotDeserializer();
        Map<String, String> config = new HashMap<>();
        config.put("some key", null);

        //when
        Long actual = underTest.getLongValue(config, "some key", 99L);

        //then
        Assert.assertThat(actual, is(99L));
    }
    
    class SnapshotDeserializer extends AbstractSnapshotDeserializer{

        @Override
        protected Object getParsedSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
            return null;
        }

        @Override
        protected Object doDeserialize(Object input, byte protocolId, SchemaMetadata schemaMetadata, Integer writerSchemaVersion, Integer readerSchemaVersion) throws SerDesException {
            return null;
        }

        @Override
        protected byte retrieveProtocolId(Object input) throws SerDesException {
            return 0;
        }

        @Override
        protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, Object input) throws SerDesException {
            return null;
        }

        @Override
        public Object deserialize(Object input, Object readerSchemaInfo) throws SerDesException {
            return null;
        }
    }
}