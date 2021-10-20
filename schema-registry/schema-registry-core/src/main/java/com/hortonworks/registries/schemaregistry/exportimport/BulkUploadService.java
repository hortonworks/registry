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
 **/
package com.hortonworks.registries.schemaregistry.exportimport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.exportimport.reader.ClouderaFileReader;
import com.hortonworks.registries.schemaregistry.exportimport.reader.ConfluentFileReader;
import com.hortonworks.registries.storage.exception.AlreadyExistsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BulkUploadService {

    private static final Logger LOG = LoggerFactory.getLogger(BulkUploadService.class);

    private final ISchemaRegistry schemaRegistry;

    public BulkUploadService(ISchemaRegistry schemaRegistry) {
        this.schemaRegistry = checkNotNull(schemaRegistry, "schemaRegistry");
    }
    public UploadResult bulkUploadSchemas(InputStream file, boolean failOnError, BulkUploadInputFormat format) {
        switch (format) {
            case CONFLUENT: return bulkUploadConfluentSchemas(file, failOnError);
            case CLOUDERA: return bulkUploadClouderaSchemas(file, failOnError);
            default: throw new IllegalArgumentException("BulkUploadInputFormat is not supported for type " + format);
        }
    }

    private UploadResult bulkUploadClouderaSchemas(InputStream file, boolean failOnError) {
        AtomicInteger successCount = new AtomicInteger();
        List<Long> failedIds = new ArrayList<>();
        ClouderaFileReader reader = new ClouderaFileReader(file);
        List<AggregatedSchemaMetadataInfo> metadataInfos = reader.getMetadataInfos();
        metadataInfos.stream()
            .sorted(Comparator.comparingLong(AggregatedSchemaMetadataInfo::getId))
            .forEach(info -> {
            SchemaMetadataInfo existingSchemaMetadata = schemaRegistry.getSchemaMetadataInfo(info.getId());
            if (existingSchemaMetadata == null) {
                if (schemaRegistry.getSchemaMetadataInfo(info.getSchemaMetadata().getName()) == null) {
                    LOG.debug("Adding SchemaMetadata with id {}", info.getId());
                    schemaRegistry.addSchemaMetadataWithoutBranch(info::getId, info.getSchemaMetadata(), true);
                } else {
                    LOG.debug("SchemaMetadata with name {} already exists.", info.getSchemaMetadata().getName());
                    failedIds.addAll(collectAllIdsForFailure(info));
                }
            } else if (!(existingSchemaMetadata.getSchemaMetadata().getType().equals(info.getSchemaMetadata().getType()))) {
                failedIds.addAll(collectAllIdsForFailure(info));
                return;
            } 
            Collection<AggregatedSchemaBranch> schemaBranches = info.getSchemaBranches();
            schemaBranches.stream()
                .sorted(Comparator.comparingLong(aggregatedSchemaBranch -> aggregatedSchemaBranch.getSchemaBranch().getId()))
                .forEach(branch -> importOneBranch(branch, info, failedIds, successCount));
        });
        List<Long> failedIdsWithoutDuplicates = new ArrayList<>(new HashSet<>(failedIds));
        return new UploadResult(successCount.get(), failedIdsWithoutDuplicates.size(), failedIdsWithoutDuplicates);
    }

    private void importOneBranch(AggregatedSchemaBranch branch, AggregatedSchemaMetadataInfo info, List<Long> failedIds, AtomicInteger successCount) {
        SchemaBranch existingBranch = null;
        try {
            existingBranch = schemaRegistry.getSchemaBranch(branch.getSchemaBranch().getId());
        } catch (SchemaBranchNotFoundException e) {
            try {
                LOG.debug("SchemaBranch with id {} does not exist yet", branch.getSchemaBranch().getId());
                if (branch.getSchemaBranch().getName().equals(SchemaBranch.MASTER_BRANCH)) {
                    existingBranch = schemaRegistry.createMasterBranch(branch.getSchemaBranch().getId(), info.getId());
                } else {
                    existingBranch = schemaRegistry.createSchemaBranch(branch.getRootSchemaVersion(), branch.getSchemaBranch());
                }
                LOG.debug("SchemaBranch with id {} created", branch.getSchemaBranch().getId());
            } catch (SchemaNotFoundException | SchemaBranchNotFoundException | SchemaBranchAlreadyExistsException ex) {
                LOG.error("Should not reach this point ever, exception while adding branch with id {}", branch.getSchemaBranch().getId(), ex);
            }
        }
        if (existingBranch != null) {
            SchemaBranch finalExistingBranch = existingBranch;
            branch.getSchemaVersionInfos().stream()
                .sorted(Comparator.comparingLong(SchemaVersionInfo::getId))
                .forEach(schemaVersionInfo -> {
                    try {
                        handleAlreadyExistingSchemaVersion(schemaVersionInfo, failedIds);
                    } catch (SchemaNotFoundException schemaNotFoundException) {
                        createNewSchemaVersion(schemaVersionInfo, info, finalExistingBranch, successCount, failedIds);
                    }
                });
        }
    }

    private Set<Long> collectAllIdsForFailure(AggregatedSchemaMetadataInfo info) {
        return info.getSchemaBranches().stream()
            .flatMap(b -> b.getSchemaVersionInfos().stream())
            .map(SchemaVersionInfo::getId)
            .collect(Collectors.toSet());
    }

    private void handleAlreadyExistingSchemaVersion(SchemaVersionInfo schemaVersionInfo, List<Long> failedIds) throws SchemaNotFoundException {
        SchemaVersionInfo existingVersion = schemaRegistry.getSchemaVersionInfo(new SchemaIdVersion(schemaVersionInfo.getId()));
        LOG.debug("SchemaVersionInfo with id {} already exists: {}", existingVersion.getId(), existingVersion);
        if (!existingVersion.getSchemaText().equals(schemaVersionInfo.getSchemaText())) {
            LOG.debug("Already existing SchemaVersionInfo with id {} does not have same schema text, adding id to failedId-s", existingVersion.getId());
            failedIds.add(schemaVersionInfo.getId());
        } else {
            LOG.debug("SchemaVersionInfo with id {} already exists and they have the same schemaText", schemaVersionInfo.getId());
        }
    }

    private void createNewSchemaVersion(SchemaVersionInfo schemaVersionInfo, AggregatedSchemaMetadataInfo info, SchemaBranch finalExistingBranch, AtomicInteger successCount, List<Long> failedIds) {
        LOG.debug("SchemaVersionInfo with id {} does not exist yet", schemaVersionInfo.getId());
        try {
            schemaRegistry.addSchemaVersionWithBranchName(finalExistingBranch.getName(), info.getSchemaMetadata(), schemaVersionInfo.getId(),
                schemaVersionInfo); 
            LOG.debug("Added SchemaVersionInfo with id {}", schemaVersionInfo.getId());
            successCount.getAndIncrement();
        } catch (IncompatibleSchemaException | InvalidSchemaException | SchemaNotFoundException ex) {
            LOG.error("Exception while adding version with id {}", schemaVersionInfo.getId(), ex);
            failedIds.add(schemaVersionInfo.getId());
        }
    }

    /**
     * Parse the input file and upload the schemas to the currently running Schema Registry's database.
     */
    public UploadResult bulkUploadConfluentSchemas(InputStream file, boolean failOnError) {
        
        Multimap<SchemaMetadataInfo, SchemaVersionInfo> schemasToUpload = ArrayListMultimap.create();
        int successCount = 0;
        List<Long> failedIds = new ArrayList<>();

        ConfluentFileReader reader = new ConfluentFileReader(file);

        RawSchema rawSchema;
        while ((rawSchema = reader.readSchema()) != null) {
            boolean ok = true;
            if (!schemasToUpload.containsKey(rawSchema.getMetadata())) {
                ok = validateSchemaMetadata(rawSchema.getMetadata());
            }

            ok &= validateSchemaVersion(rawSchema.getVersion());
            if (!ok) {
                failedIds.add(rawSchema.getMetadata().getId());
            } else {
                successCount++;
                schemasToUpload.put(rawSchema.getMetadata(), rawSchema.getVersion());
            }
        }

        // do not continue unless the user asked for it
        if (failOnError && failedIds.size() > 0) {
            return new UploadResult(successCount, failedIds.size(), failedIds);
        }

        return uploadValidSchemas(schemasToUpload, failedIds);
    }

    /**
     * Check if the schema with this name or id already exists in the target database.
     * @param metadata  uploaded schema metadata
     * @return  true if schema is valid (does not exist in target), false otherwise
     */
    @VisibleForTesting
    boolean validateSchemaMetadata(SchemaMetadataInfo metadata) {
        SchemaMetadataInfo meta = schemaRegistry.getSchemaMetadataInfo(metadata.getSchemaMetadata().getName());
        if (meta != null) {
            return false;
        }
        if (metadata.getId() != null) {
            meta = schemaRegistry.getSchemaMetadataInfo(metadata.getId());
        }
        return meta == null;
    }

    /**
     * Check if the schema version contains valid data.
     * @param version   uploaded schema version
     * @return  true if the version is valid, false otherwise
     */
    @VisibleForTesting
    boolean validateSchemaVersion(SchemaVersionInfo version) {
        return StringUtils.isNotBlank(version.getSchemaText());
    }

    /**
     * Upload the provided schemas. We are assuming these have been pre-validated. It is still
     * possible for the database to fail for various reasons, so this method returns a statistic
     * of how many schemas were successfully inserted and how many failed.
     */
    @VisibleForTesting
    UploadResult uploadValidSchemas(Multimap<SchemaMetadataInfo, SchemaVersionInfo> schemasToUpload,
                                    List<Long> failedIds) {
        int successCount = 0;

        for (SchemaMetadataInfo meta : schemasToUpload.keySet()) {
            Long schemaId;
            try {
                LOG.info("Adding {}", meta.getSchemaMetadata());
                schemaId = schemaRegistry.addSchemaMetadata(meta.getId(), meta.getSchemaMetadata());
            } catch (UnsupportedSchemaTypeException | AlreadyExistsException ex) {
                failedIds.add(meta.getId());
                LOG.error("Could not add new schema metadata {}", meta, ex);
                continue;  // skip all versions of this meta
            }

            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaId);
            if (schemaMetadataInfo == null) {
                failedIds.add(schemaId);
                continue;
            }
            SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
            if (schemaMetadata == null) {
                failedIds.add(schemaId);
                continue;
            }

            Collection<SchemaVersionInfo> versions = schemasToUpload.get(meta);
            for (SchemaVersionInfo version : versions) {
                try {
                    LOG.info("Adding version {} to schema {}", version.getVersion(), schemaMetadata.getName());
                    SchemaIdVersion schemaIdVersion = schemaRegistry.addSchemaVersionWithBranchName(
                            "MASTER",
                            schemaMetadata,
                            version.getId(),
                            version
                    );
                    checkState(schemaIdVersion.getVersion().equals(version.getVersion()),
                            "Version not same after upload: %s vs %s",
                            schemaIdVersion.getSchemaVersionId(), version.getId());
                    successCount++;
                } catch (Exception ex) {
                    failedIds.add(schemaId);
                    LOG.error("Error while adding new version for schema {}: {}", meta.getSchemaMetadata().getName(),
                            version, ex);
                }
            }
        }

        return new UploadResult(successCount, failedIds.size(), failedIds);
    }

}
