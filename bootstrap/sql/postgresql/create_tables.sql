-- Copyright 2016 Hortonworks.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- THE NAMES OF THE TABLE COLUMNS MUST MATCH THE NAMES OF THE CORRESPONDING CLASS MODEL FIELDS

CREATE TABLE IF NOT EXISTS schema_metadata_info (
  "id"            SERIAL PRIMARY KEY,
  "type"          VARCHAR(256)          NOT NULL,
  "schemaGroup"   VARCHAR(256)          NOT NULL,
  "name"          VARCHAR(256)          NOT NULL,
  "compatibility" VARCHAR(256)          NOT NULL,
  "description"   TEXT,
  "evolve"        BOOLEAN               NOT NULL,
  "timestamp"     BIGINT                NOT NULL,
  UNIQUE("id","name")
);

-- Version 0.3.0
ALTER TABLE schema_metadata_info
  ADD validationLevel IF NOT EXISTS VARCHAR(256) NOT NULL DEFAULT 'ALL';

CREATE TABLE IF NOT EXISTS schema_version_info (
  "id"            SERIAL UNIQUE NOT NULL,
  "description"    TEXT,
  "schemaText"     TEXT                  NOT NULL,
  "fingerprint"    TEXT                  NOT NULL,
  "version"          INT                   NOT NULL,
  "schemaMetadataId" BIGINT                NOT NULL,
  "timestamp"        BIGINT                NOT NULL,
  "name"             VARCHAR(256)          NOT NULL,
  UNIQUE("schemaMetadataId", "version"),
  PRIMARY KEY ("name", "version"),
  FOREIGN KEY ("schemaMetadataId", "name") REFERENCES schema_metadata_info("id", "name")
);

CREATE TABLE IF NOT EXISTS schema_field_info (
  "id"               SERIAL PRIMARY KEY,
  "schemaInstanceId" BIGINT NOT NULL,
  "timestamp"        BIGINT NOT NULL,
  "name"             VARCHAR(256)   NOT NULL,
  "fieldNamespace"   VARCHAR(256),
  "type"             VARCHAR(256)          NOT NULL,
  FOREIGN KEY ("schemaInstanceId") REFERENCES schema_version_info ("id")
);

CREATE TABLE IF NOT EXISTS schema_serdes_info (
  "id"                    SERIAL PRIMARY KEY,
  "description"           TEXT,
  "name"                  TEXT                  NOT NULL,
  "fileId"                TEXT                  NOT NULL,
  "serializerClassName"   TEXT                  NOT NULL,
  "deserializerClassName" TEXT                  NOT NULL,
  "timestamp"             BIGINT                NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_serdes_mapping (
  "schemaMetadataId" BIGINT NOT NULL,
  "serDesId"         BIGINT NOT NULL,

  UNIQUE ("schemaMetadataId", "serDesId")
);
