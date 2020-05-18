package com.hortonworks.registries.schemaregistry.avro;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.BadRequestException;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.helper.JarFileFactory;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.util.SchemaRegistryTestName;

@Category(IntegrationTest.class)
public class FileUploadTest {
    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;
    private static SchemaRegistryClient SCHEMA_REGISTRY_CLIENT;
    @Rule
    public SchemaRegistryTestName TEST_NAME_RULE = new SchemaRegistryTestName();

    @BeforeClass
    public static void setup() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(
                SchemaRegistryTestProfileType.DEFAULT);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        SCHEMA_REGISTRY_CLIENT = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient(false);
    }

    private String uploadValidFile() throws IOException {
        InputStream inputStream = new FileInputStream(JarFileFactory.createValidJar());
        return SCHEMA_REGISTRY_CLIENT.uploadFile(inputStream);
    }

    private String uploadInvalidFile() throws IOException {
        InputStream inputStream = new FileInputStream(JarFileFactory.createCorruptedJar());
        return SCHEMA_REGISTRY_CLIENT.uploadFile(inputStream);
    }

    @Test
    public void uploadValidFileResults() throws Exception {
        // when
        uploadValidFile();
        
        // then no exception is thrown
    }

    @Test(expected = BadRequestException.class)
    public void uploadInvalidFileResultsBadRequest() throws Exception {
        // when
        uploadInvalidFile();
    }
}
