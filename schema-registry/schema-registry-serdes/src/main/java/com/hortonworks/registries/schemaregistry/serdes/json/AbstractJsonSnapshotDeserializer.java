/*
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
 */
package com.hortonworks.registries.schemaregistry.serdes.json;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.json.JsonSchemaResolver;
import com.hortonworks.registries.schemaregistry.serde.AbstractSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.SerDesProtocolHandler;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractJsonSnapshotDeserializer<I> extends AbstractSnapshotDeserializer<I, Object, Schema> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJsonSnapshotDeserializer.class);

  private static final String JSON_SCHEMA = "json.schema";

  protected JsonSchemaResolver jsonSchemaResolver;
  protected JsonSerDesHandler serDesHandler = new JsonSerDesHandler();

  public AbstractJsonSnapshotDeserializer() {
    super();
  }

  public AbstractJsonSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }

  @Override
  public void doInit(Map<String, ?> config) {
    super.doInit(config);
    SchemaVersionRetriever schemaVersionRetriever = createSchemaVersionRetriever();
    jsonSchemaResolver = new JsonSchemaResolver(schemaVersionRetriever);
  }

  private SchemaVersionRetriever createSchemaVersionRetriever() {
    return new SchemaVersionRetriever() {
      @Override
      public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
        return schemaRegistryClient.getSchemaVersionInfo(key);
      }

      @Override
      public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
        return schemaRegistryClient.getSchemaVersionInfo(key);
      }
    };
  }

  @Override
  protected Schema getParsedSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
    JSONObject rawSchema = new JSONObject(jsonSchemaResolver.resolveSchema(schemaVersionKey));
    return SchemaLoader.load(rawSchema);
  }

  protected Object buildDeserializedObject(byte protocolId,
                                           InputStream payloadInputStream,
                                           SchemaMetadata schemaMetadata,
                                           Integer schemaVersion) throws SerDesException {



    String schemaName = schemaMetadata.getName();
    SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, schemaVersion);
    LOG.debug("SchemaKey: [{}] for the received payload", schemaVersionKey);
    Schema writerSchema = getSchema(schemaVersionKey);
    if (writerSchema == null) {
      throw new RegistryException("No schema exists with metadata-key: " + schemaMetadata + " and schemaVersion: " + schemaVersionKey);
    }

    return deserializePayloadForProtocol(protocolId, payloadInputStream, writerSchema);
  }

  protected Object deserializePayloadForProtocol(byte protocolId,
                                                 InputStream payloadInputStream,
                                                 Schema schema) throws SerDesException  {
    Map<String, Object> props = new HashMap<>();
    props.put(JSON_SCHEMA, schema);
    SerDesProtocolHandler serDesProtocolHandler = SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId);

    return serDesProtocolHandler.handlePayloadDeserialization(payloadInputStream, props);
  }
  
}
