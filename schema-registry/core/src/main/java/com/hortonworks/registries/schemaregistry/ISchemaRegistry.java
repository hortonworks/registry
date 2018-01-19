/**
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry;

import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.WhereClause;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface ISchemaRegistry extends ISchemaRegistryService {

    String SCHEMA_PROVIDERS = "schemaProviders";

    /**
     * initializes it with the given properties
     *
     * @param props properties to be initialized with.
     */
    void init(Map<String, Object> props);

    /**
     * Registers information about a schema if it is not yet and returns it's identifier.
     *
     * @param schemaMetadata     metadata about schema.
     * @param throwErrorIfExists whether to throw an error if it already exists.
     *
     * @return id of the registered schema which is successfully registered now or earlier.
     */
    Long addSchemaMetadata(SchemaMetadata schemaMetadata, boolean throwErrorIfExists);

    /**
     * If there is a version of the schema with the given schemaText for schema name then it returns respective {@link SchemaVersionInfo},
     * else it returns null.
     *
     * @param schemaName name of the schema
     * @param schemaText text of the schema
     *
     * @return SchemaVersionInfo instance about the registered version of schema which is same as the given {@code schemaText}
     *
     * @throws SchemaNotFoundException when no schema metadata registered with the given schema name.
     * @throws InvalidSchemaException  when the given {@code schemaText} is not valid.
     */
    SchemaVersionInfo getSchemaVersionInfo(String schemaName, String schemaText) throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException;

    /**
     * @param props properties
     *
     * @return Collects aggregated schema metadata which contains the given properties.
     */
    Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> props) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param schemaName name of the schema
     *
     * @return {@link AggregatedSchemaMetadataInfo} for the given schema name, null if there is no schema registered with
     * the given schema name.
     */
    AggregatedSchemaMetadataInfo getAggregatedSchemaMetadataInfo(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    /**
     * @param props properties
     *
     * @return All SchemaMetadata having the given properties.
     */
    Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> props);

    /**
     * @param serDesId id
     *
     * @return SerDesInfo for the given serDesId, null if it does not exist.
     */
    SerDesInfo getSerDes(Long serDesId);

    /**
     * Searches the registry to find schemas according to the given {@code whereClause} and orders the results by given {@code orderByFields}
     *
     * @param whereClause
     * @param orderByFields
     *
     * @return Collection of schemas from the results of given where clause.
     */
    Collection<SchemaMetadataInfo> searchSchemas(WhereClause whereClause, List<OrderBy> orderByFields);

    /**
     *  Merges a given schema version to 'MASTER' branch with a merge strategy
     * @param schemaVersionId             id of the schema version to be merged
     * @param schemaVersionMergeStrategy  merge strategy to be used for merging to 'MASTER'
     * @return
     * @throws SchemaNotFoundException
     * @throws IncompatibleSchemaException
     */
    default SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, SchemaVersionMergeStrategy schemaVersionMergeStrategy) throws SchemaNotFoundException, IncompatibleSchemaException {
        throw new UnsupportedOperationException();
    }


    /**
     * @param schemaName name identifying a schema
     *
     * @return all schema branches with versions of the schemas for given schemaName
     *
     * @throws SchemaNotFoundException if there is no schema metadata registered with the given {@code schemaName}
     */
    Collection<AggregatedSchemaBranch> getAggregatedSchemaBranch(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException;


    /**
     *  Invalidates a cache entry given its cache type and its key, invalidates all entries in all the caches if the cache type is 'ALL'
     *
     * @param schemaRegistryCacheType cache type
     * @param keyAsString serialized version of the key of the cache
     */
    void invalidateCache(SchemaRegistryCacheType schemaRegistryCacheType, String keyAsString);

    /**
     *  A node which comes online in HA mode will notify all the existing node about its presence in the cluster, existing node will update their in memory cache of the complete list of nodes in HA mode.
     * @param nodeUrl URL of the node making a debut in a HA environment
     */
    void registerNodeDebut(String nodeUrl);

}