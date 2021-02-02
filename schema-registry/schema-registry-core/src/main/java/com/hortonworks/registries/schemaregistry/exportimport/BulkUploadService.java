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
package com.hortonworks.registries.schemaregistry.exportimport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.exportimport.reader.ClouderaFileReader;
import com.hortonworks.registries.schemaregistry.exportimport.reader.ConfluentFileReader;
import com.hortonworks.registries.schemaregistry.exportimport.reader.UploadedFileReader;
import com.hortonworks.registries.storage.exception.AlreadyExistsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BulkUploadService {

    private static final Logger LOG = LoggerFactory.getLogger(BulkUploadService.class);

    private final ISchemaRegistry schemaRegistry;

    public BulkUploadService(ISchemaRegistry schemaRegistry) {
        this.schemaRegistry = checkNotNull(schemaRegistry, "schemaRegistry");
    }

    /**
     * Parse the input file and upload the schemas to the currently running Schema Registry's database.
     */
    public UploadResult bulkUploadSchemas(InputStream file, boolean failOnError, BulkUploadInputFormat format) throws IOException {
        Multimap<SchemaMetadataInfo, SchemaVersionInfo> schemasToUpload = ArrayListMultimap.create();
        int successCount = 0;
        List<Long> failedIds = new ArrayList<>();

        UploadedFileReader reader = getReader(file, format);

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

    private UploadedFileReader getReader(InputStream file, BulkUploadInputFormat format) throws IOException {
        switch (format) {
            case CLOUDERA:
                return new ClouderaFileReader(file);
            case CONFLUENT:
                return new ConfluentFileReader(file);
            default:
                throw new Error("Unsupported reader: " + format);
        }
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
                    SchemaIdVersion schemaIdVersion = schemaRegistry.addSchemaVersion(
                            schemaMetadata, version.getId(),
                            new SchemaVersion(version.getSchemaText(), version.getDescription()));
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
