/**
 * Copyright 2016-2020 Cloudera, Inc.
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
package com.cloudera.dim.atlas;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.Map;
import java.util.Optional;

/**
 * <p>Interface between Schema Registry and Atlas.</p>
 * <p>
 *     We used a plugin solution for interfacing with Atlas in order to avoid classpath collisions. Schema Registry
 *     has its own set of dependencies (Dropwizard, Jersey) which are in collision with the dependencies brought in
 *     by Atlas. For this reason the Atlas code is separately plugged in via a classpath loader.
 * </p>
 * @see com.cloudera.dim.atlas.shim.AtlasPluginFactory
 */
public interface AtlasPlugin {

    /** Configuration parameter used in <pre>registry.yaml</pre> */
    String ATLAS_HOSTS_PARAM = "atlasUrls";

    String ATLAS_BASIC_AUTH = "basicAuth";

    /** Initialize the plugin. This method should be called by the plugin factory or any other
     * place where the plugin instance is created. */
    void initialize(Map<String, Object> config);

    /**
     * Create the SchemaRegistry type model in Atlas. This method should only be called once, during initialization.
     * <p>
     *     Similarly to relational databases, in Atlas we need to define a type model before we can use it.
     *     We need a Meta type, a Version type, etc. In classical RDBMS these are tables, in Atlas they are
     *     types. Other than entities, we also need to define relationship types, which can be thought of as
     *     foreign key contraints. <br> Once defined, we can create entities and relationships of given types.
     * </p>
     */
    void setupAtlasModel();

    /**
     * Create a relationship between the kafka_topic entity and schema_metadata_info. This only works if
     * the kafka_topic type exists. If it doesn't, the <tt>false</tt> is returned.
     */
    boolean setupKafkaSchemaModel();

    /**
     * Have we set up a relationship between kafka_topic and schema_metadata_info entity types?
     */
    boolean isKafkaSchemaModelInitialized();

    /**
     * Connect schema metadata with kafka topic
     * @param metaGuid              the GUID of the metadata Atlas entity
     * @param schemaMetadataInfo    additional informaiton about the schema
     */
    void connectSchemaWithTopic(String metaGuid, SchemaMetadataInfo schemaMetadataInfo);

    /**
     * Create a schema meta entity and immediately assign a MASTER branch to it.
     *
     * @param meta  object containing information for creating the schema meta
     * @return the GUID of the new Atlas entity
     */
    String createMeta(SchemaMetadataInfo meta);

    /**
     * Update an existing schema meta.
     * @param schemaMetadata    Object containing information for updating the meta. Note that the <b>name</b> of
     *                          the meta must be an existing schema's name.
     * @return      If the update was successful, it will return the updated entity. If it failed then none will be returned.
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    Optional<SchemaMetadataInfo> updateMeta(SchemaMetadata schemaMetadata) throws SchemaNotFoundException;

    /**
     * Add a new version to an existing schema.
     * @param schemaName        name of the existing schema
     * @param schemaVersion     object containing information about the version
     * @return  the unique ID of the new version
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    void addSchemaVersion(String schemaName, SchemaVersionInfo schemaVersion) throws SchemaNotFoundException;

}
