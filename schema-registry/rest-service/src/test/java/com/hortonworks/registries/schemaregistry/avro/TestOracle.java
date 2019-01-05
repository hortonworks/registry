package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class TestOracle {
    //private static final String SCHEMA_REGISTRY_URL = "https://apimgrtr.bnsf.com/hsr/api/v1/";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:9090/api/v1/";

    private SchemaRegistryClient schemaRegistryClient;

    private String schemaTemplate;

    @Before
    public void setup() throws IOException {
        HortonworksSecuredEventSchemaClientFactory factory = new HortonworksSecuredEventSchemaClientFactory();

       // URL resource = TestOracle.class.getResource("/bnsf-trusted.jks");

        Properties configurations = new Properties();
        configurations.put("schema.registry.url", SCHEMA_REGISTRY_URL);
//        configurations.put("schema.registry.client.ssl.trustStoreType", "JKS");
//        configurations.put("schema.registry.client.ssl.trustStorePath", resource.getPath());
//        configurations.put("schema.registry.client.ssl.trustStorePassword", "changeit");
//        configurations.put("schema.registry.client.ssl.keyStorePath", "NONE");
//        configurations.put("schema.registry.client.ssl.keyStorePassword", "");
//        configurations.put("schema.registry.client.http.basic.authentication.username", "ytevtadm");
//        configurations.put("schema.registry.client.http.basic.authentication.password", "aM7o39Dz");

        schemaRegistryClient = factory.createEventSchemaClient(configurations);
        schemaTemplate = IOUtils.toString(TestOracle.class.getClassLoader().getResourceAsStream("SchemaTemplate.avsc"), Charset.defaultCharset());
    }

    @Test
    public void publishAll() throws SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS");
        for (int i = 0; i < 100; i++) {
            String name = "TestName_" + format.format(new Date());
            String namespace = "com.bnsf.tsm.event.test";
            String content = schemaTemplate
                    .replace("${name}", name)
                    .replace("${namespace}", namespace);


            EventSchemaVersion schemaVersion =
                    new EventSchemaVersion(
                            name,
                            "Version 1",
                            namespace,
                            content,
                            1,
                            Type.AVRO);

            System.out.println("Trying to publish schema - " + name);
            schemaRegistryClient.addSchemaMetadata(buildSchemaMetadata(schemaVersion));
            schemaRegistryClient.addSchemaVersion(schemaVersion.getName(), new SchemaVersion(schemaVersion.getContent(), String.valueOf(schemaVersion.getVersion())));
            System.out.println("Successfully published schema - " + name);
        }
    }

    private SchemaMetadata buildSchemaMetadata(EventSchemaVersion eventSchemaVersion) {
        return (new SchemaMetadata.Builder(eventSchemaVersion.getName())).description(eventSchemaVersion.getDescription()).schemaGroup(eventSchemaVersion.getGroup()).evolve(true).type(eventSchemaVersion.getType().value()).validationLevel(SchemaValidationLevel.ALL).compatibility(SchemaCompatibility.BOTH).build();
    }
}
