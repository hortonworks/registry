-- Copyright 2016 Hortonworks.;
-- ;
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.;
-- You may obtain a copy of the License at;
-- ;
--    http://www.apache.org/licenses/LICENSE-2.0;
-- ;
-- Unless required by applicable law or agreed to in writing, software;
-- distributed under the License is distributed on an "AS IS" BASIS,;
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.;
-- See the License for the specific language governing permissions and;
-- limitations under the License.;
-- ;
-- THE NAMES OF THE TABLE COLUMNS MUST MATCH THE NAMES OF THE CORRESPONDING CLASS MODEL FIELDS;
-- ;
-- USE schema_registry;

-- This script makes all the necessary changes to bring the database to 3.0.2 from any of the previous versions

CREATE TABLE IF NOT EXISTS schema_metadata_info (
  "id"              SERIAL UNIQUE NOT NULL,
  "type"            VARCHAR(255) NOT NULL,
  "schemaGroup"     VARCHAR(255) NOT NULL,
  "name"            VARCHAR(255) NOT NULL,
  "compatibility"   VARCHAR(255) NOT NULL,
  "validationLevel" VARCHAR(255) NOT NULL,
  "description"     TEXT,
  "evolve"          BOOLEAN      NOT NULL,
  "timestamp"       BIGINT       NOT NULL,
  PRIMARY KEY ( "name"),
  UNIQUE ("id")
);

CREATE TABLE IF NOT EXISTS schema_version_info (
  "id"               SERIAL UNIQUE NOT NULL,
  "description"      TEXT,
  "schemaText"       TEXT          NOT NULL,
  "fingerprint"      TEXT          NOT NULL,
  "version"          INT           NOT NULL,
  "schemaMetadataId" BIGINT        NOT NULL,
  "timestamp"        BIGINT        NOT NULL,
  "name"             VARCHAR(255)  NOT NULL,
  UNIQUE ("schemaMetadataId", "version"),
  PRIMARY KEY ("name", "version"),
  FOREIGN KEY ("schemaMetadataId") REFERENCES schema_metadata_info ("id") ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY ("name") REFERENCES schema_metadata_info ("name") ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS schema_field_info (
  "id"               SERIAL PRIMARY KEY,
  "schemaInstanceId" BIGINT       NOT NULL,
  "timestamp"        BIGINT       NOT NULL,
  "name"             VARCHAR(255) NOT NULL,
  "fieldNamespace"   VARCHAR(255),
  "type"             VARCHAR(255) NOT NULL,
  FOREIGN KEY ("schemaInstanceId") REFERENCES schema_version_info ("id") ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS schema_serdes_info (
  "id"                    SERIAL PRIMARY KEY,
  "description"           TEXT,
  "name"                  TEXT   NOT NULL,
  "fileId"                TEXT   NOT NULL,
  "serializerClassName"   TEXT   NOT NULL,
  "deserializerClassName" TEXT   NOT NULL,
  "timestamp"             BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_serdes_mapping (
  "schemaMetadataId" BIGINT NOT NULL,
  "serDesId"         BIGINT NOT NULL,

  UNIQUE ("schemaMetadataId", "serDesId")
);


CREATE OR REPLACE FUNCTION add_validation_level_if_missing()
  RETURNS void AS $$
  DECLARE
    col_count INT;
  BEGIN
    SELECT COUNT(*) INTO col_count FROM information_schema."columns" WHERE table_schema = current_schema() and table_name = 'schema_metadata_info' and column_name = 'validationLevel';
    IF col_count = 0 THEN
      EXECUTE 'ALTER TABLE "schema_metadata_info" ADD COLUMN "validationLevel" VARCHAR(255) NOT NULL DEFAULT ''ALL''';
    END IF;
  END;
$$ LANGUAGE plpgsql;

SELECT add_validation_level_if_missing();