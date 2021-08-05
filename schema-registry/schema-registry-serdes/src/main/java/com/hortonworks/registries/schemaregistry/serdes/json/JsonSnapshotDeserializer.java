/**
 * Copyright 2016-2021 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.serdes.json;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.json.JsonSchemaResolver;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.schemaregistry.serdes.json.exceptions.JsonException;
import com.hortonworks.registries.schemaregistry.serdes.json.exceptions.JsonRetryableException;
import org.everit.json.schema.Schema;

import java.io.IOException;
import java.io.InputStream;

public class JsonSnapshotDeserializer extends AbstractJsonSnapshotDeserializer<InputStream> {

    public JsonSnapshotDeserializer() { }

    public JsonSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    protected JsonSnapshotDeserializer(JsonSchemaResolver schemaResolver, JsonSerDesHandler handler) {
        this.jsonSchemaResolver = schemaResolver;
        this.serDesHandler = handler;
    }

    @Override
    protected Object doDeserialize(InputStream input, byte protocolId, SchemaMetadata schemaMetadata, Integer writerSchemaVersion, Integer readerSchemaVersion) throws SerDesException {
        String schemaName = schemaMetadata.getName();
        SchemaVersionKey versionKey = new SchemaVersionKey(schemaName, writerSchemaVersion);
        try {
            Schema json = getParsedSchema(versionKey);
            return serDesHandler.handlePayloadDeserialization(input, json);
        } catch (Exception e) {
            throw new SerDesException(e);
        }
    }

    @Override
    protected byte retrieveProtocolId(InputStream input) throws SerDesException {
        byte protocolId;
        try {
            protocolId = (byte) input.read();
        } catch (IOException e) {
            throw new JsonRetryableException(e);
        }

        if (protocolId == -1) {
            throw new JsonException("End of stream reached while trying to read protocol id");
        }

        checkProtocolHandlerExists(protocolId);

        return protocolId;
    }

    private void checkProtocolHandlerExists(byte protocolId) {
        if (SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId) == null) {
            throw new JsonException("Unknown protocol id [" + protocolId + "] received while deserializing the payload");
        }
    }

    @Override
    protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, InputStream input) throws SerDesException {
        return SerDesProtocolHandlerRegistry.get()
            .getSerDesProtocolHandler(protocolId)
            .handleSchemaVersionDeserialization(input);
    }
}
