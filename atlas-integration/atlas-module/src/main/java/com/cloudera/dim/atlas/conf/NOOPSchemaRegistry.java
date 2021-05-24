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
package com.cloudera.dim.atlas.conf;

import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeStrategy;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exportimport.BulkUploadInputFormat;
import com.hortonworks.registries.schemaregistry.exportimport.UploadResult;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;

import javax.annotation.Nullable;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NOOPSchemaRegistry implements IAtlasSchemaRegistry {
    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return null;
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        return null;
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
        return null;
    }

    @Override
    public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        return null;
    }

    @Nullable
    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return null;
    }

    @Nullable
    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return null;
    }

    @Override
    public void deleteSchema(String schemaName) throws SchemaNotFoundException {

    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion, boolean disableCanonicalCheck) 
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName, SchemaMetadata schemaMetadata, 
                                            SchemaVersion schemaVersion, boolean disableCanonicalCheck) 
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion, boolean disableCanonicalCheck) 
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName, String schemaName, SchemaVersion schemaVersion, boolean disableCanonicalCheck) 
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {

    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaName, String toSchemaText) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName, String toSchemaText) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) 
            throws SchemaBranchNotFoundException, SchemaNotFoundException {
        return null;
    }

    @Override
    public String uploadFile(InputStream inputStream) throws SerDesException {
        return null;
    }

    @Override
    public InputStream downloadFile(String fileId) throws IOException {
        return null;
    }

    @Override
    public Long addSerDes(SerDesPair serializerInfo) {
        return null;
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {

    }

    @Override
    public Collection<SerDesInfo> getSerDes(String schemaName) {
        return null;
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId) 
            throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {

    }

    @Override
    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {

    }

    @Override
    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {

    }

    @Override
    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {

    }

    @Override
    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {

    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, boolean disableCanonicalCheck) 
            throws SchemaNotFoundException, IncompatibleSchemaException {
        return null;
    }

    @Override
    public void transitionState(Long schemaVersionId, Byte targetStateId, byte[] transitionDetails) 
            throws SchemaNotFoundException, SchemaLifecycleException {

    }

    @Override
    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        return null;
    }

    @Override
    public SchemaBranch createSchemaBranch(Long schemaVersionId, SchemaBranch schemaBranch) 
            throws SchemaBranchAlreadyExistsException, SchemaNotFoundException {
        return null;
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranches(String schemaName) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {

    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName, List<Byte> stateIds) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata, boolean throwErrorIfExists) {
        return null;
    }

    @Override
    public SchemaVersionInfo getLatestEnabledSchemaVersionInfo(String schemaBranchName, String schemaName) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(String schemaName, String schemaText, boolean disableCanonicalCheck) 
            throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo findSchemaVersionByFingerprint(String fingerprint) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> props) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public AggregatedSchemaMetadataInfo getAggregatedSchemaMetadataInfo(String schemaName) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> props) {
        return null;
    }

    @Override
    public SerDesInfo getSerDes(Long serDesId) {
        return null;
    }

    @Override
    public Collection<SchemaMetadataInfo> searchSchemas(MultivaluedMap<String, String> queryParameters, Optional<String> orderBy) {
        return null;
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, SchemaVersionMergeStrategy schemaVersionMergeStrategy, 
                                                       boolean disableCanonicalCheck) 
            throws IncompatibleSchemaException, SchemaNotFoundException {
        return null;
    }

    @Override
    public Collection<AggregatedSchemaBranch> getAggregatedSchemaBranch(String schemaName) 
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaBranch getSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranchesForVersion(Long vertionId) throws SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public void invalidateCache(SchemaRegistryCacheType schemaRegistryCacheType, String keyAsString) {

    }

    @Override
    public UploadResult bulkUploadSchemas(InputStream file, boolean failOnError, BulkUploadInputFormat format) throws IOException {
        return null;
    }

    @Override
    public Long addSchemaMetadata(Long id, SchemaMetadata schemaMetadata) {
        return null;
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, Long versionId, SchemaVersion schemaVersion) 
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
        return null;
    }

    @Override
    public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
        return null;
    }
}
