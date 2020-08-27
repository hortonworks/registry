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

import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.Collection;
import java.util.List;
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
    String ATLAS_HOSTS_PARAM = "urls";

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
     * Create a schema meta entity and immediately assign a MASTER branch to it.
     *
     * @param meta  object containing information for creating the schema meta
     * @return  the unique ID of the newly created meta
     */
    Long createMeta(SchemaMetadata meta);

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
     * @param fingerprint       the schema text hash
     * @param schemaBranch      each version must belong to a branch (default: MASTER)
     * @return  the unique ID of the new version
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion, String fingerprint, SchemaBranch schemaBranch) throws SchemaNotFoundException;

    /**
     * Get schema by its unique name. If it's not found then none will be returned.
     */
    Optional<SchemaMetadataInfo> getSchemaMetadataInfo(String schemaName);

    /**
     * Get schema by its unique ID. If it's not found then none will be returned.
     */
    Optional<SchemaMetadataInfo> getSchemaMetadataInfo(Long metaId);

    /**
     * Search for schemas. If no parameters are provided then it will return all schemas in the database.
     */
    Collection<SchemaMetadataInfo> search(Optional<String> name, Optional<String> desc, Optional<String> orderBy);

    /**
     * Find schema versions by the schema name and the fingerprint. It is possible for multiple versions to
     * have the same fingerprint (eg. in version 1 and 3 have the same text), which is why this method
     * returns a collection.
     *
     * @param schemaName        unique name of the schema
     * @param fingerprint       hash of the schema text
     * @return      collection of versions (can be empty)
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    Collection<SchemaVersionInfo> searchVersions(String schemaName, String fingerprint) throws SchemaNotFoundException;

    /** TODO This method has not been implemented yet. */
    Collection<SerDesInfo> getSerDesMappingsForSchema(String schemaName) throws SchemaNotFoundException;

    /**
     * Get a schema version identified by the schema name and the version number.
     *
     * @param schemaName    unique name of the schema
     * @param version       version number (eg. 1, 2, 3)
     * @return  schema version if it exists, otherwise none
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    Optional<SchemaVersionInfo> getSchemaVersion(String schemaName, Integer version) throws SchemaNotFoundException;

    /**
     * Get a schema version identified by its unique ID.
     * <p>A version has a version number (1,2,3) and a unique ID. While the version numbers are only
     * unique per each schema, the IDs are unique globally.</p>
     *
     * @param versionId     version id
     * @return  schema version if it exists, otherwise none
     */
    Optional<SchemaVersionInfo> getSchemaVersionById(Long versionId);

    /**
     * Get all the schema versions for the particular schema.
     * @param schemaName    unique name of the schema
     * @return      collection of versions
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    Collection<SchemaVersionInfo> getAllSchemaVersions(String schemaName) throws SchemaNotFoundException;

    /**
     * Get schema versions by the unique branch ID.
     *
     * @param branchId      unique ID of the branch
     * @return  list of versions belonging to a branch (can be empty)
     * @throws SchemaBranchNotFoundException    If the branch with the given ID does not exist.
     */
    List<SchemaVersionInfo> getSchemaVersionsByBranchId(Long branchId) throws SchemaBranchNotFoundException;

    /**
     * A schema version can have multiple branches. This method retrieves all the branches belonging to
     * the version with the provided id.
     *
     * @param versionId     unique ID of the schema version
     * @return      list of branches (can be empty)
     * @throws SchemaBranchNotFoundException    If there is no schema version with the given ID.
     */
    Collection<SchemaBranch> getSchemaBranchesByVersionId(Long versionId) throws SchemaBranchNotFoundException;

    /**
     * Create a new branch for the schema version.
     *
     * @param schemaVersion     Object containing information about the schema version. Note that the schema name must exist.
     * @param branchName        Name of the new branch.
     * @return      Information about the new branch as it was persisted in the database.
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    SchemaBranch createBranch(SchemaVersionInfo schemaVersion, String branchName) throws SchemaNotFoundException;

    /**
     * Get a schema branch identified by the schema name and the branch name.
     *
     * @param schemaName    unique name of the schema
     * @param branchName    unique name of the branch
     * @return      returns the branch if it exists, otherwise none
     * @throws SchemaNotFoundException  If there is no schema with the given name.
     */
    Optional<SchemaBranch> getSchemaBranch(String schemaName, String branchName) throws SchemaNotFoundException;

    /**
     * Get a schema branch identified by its unique branch ID.
     * @param branchId      unique id of the branch
     * @return  returns the branch if it exists, otherwise none
     */
    Optional<SchemaBranch> getSchemaBranchById(Long branchId);

    /** Create a new SerDes in the database. The return value is the ID of the newly created entity. */
    Long addSerdes(SerDesPair serializerInfo);

    /** Get SerDes by its unique ID. */
    Optional<SerDesInfo> getSerdesById(Long serDesId);

    /**
     * Map an existing SerDes to an existing schema.
     *
     * @param schemaName    unique name of the schema
     * @param serDesId      unique id of the SerDes
     * @throws SchemaNotFoundException      If there is no schema with the given name.
     */
    void mapSchemaWithSerdes(String schemaName, Long serDesId) throws SchemaNotFoundException;

    /**
     * Get all the existing SerDes entitied mapped to a particular schema.
     *
     * @param schemaName        unique name of the schema
     * @return      collection of SerDes entities mapped to the schema
     * @throws SchemaNotFoundException      If there is no schema with the given name.
     */
    Collection<SerDesInfo> getAllSchemaSerdes(String schemaName) throws SchemaNotFoundException;

}
