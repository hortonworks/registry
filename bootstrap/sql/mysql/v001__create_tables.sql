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
  id              BIGINT AUTO_INCREMENT NOT NULL,
  type            VARCHAR(255)          NOT NULL,
  schemaGroup     VARCHAR(255)          NOT NULL,
  name            VARCHAR(255)          NOT NULL,
  compatibility   VARCHAR(255)          NOT NULL,
  validationLevel VARCHAR(255)          NOT NULL,
  description     TEXT,
  evolve          BOOLEAN               NOT NULL,
  timestamp       BIGINT                NOT NULL,
  PRIMARY KEY (name),
  UNIQUE KEY (id)
);

CREATE TABLE IF NOT EXISTS schema_version_info (
  id               BIGINT AUTO_INCREMENT NOT NULL,
  description      TEXT,
  schemaText       TEXT                  NOT NULL,
  fingerprint      TEXT                  NOT NULL,
  version          INT                   NOT NULL,
  schemaMetadataId BIGINT                NOT NULL,
  timestamp        BIGINT                NOT NULL,
  name             VARCHAR(255)          NOT NULL,
  UNIQUE KEY (id),
  UNIQUE KEY `UK_METADATA_ID_VERSION_FK` (schemaMetadataId, version),
  PRIMARY KEY (name, version),
  FOREIGN KEY (schemaMetadataId, name) REFERENCES schema_metadata_info (id, name) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS schema_field_info (
  id               BIGINT AUTO_INCREMENT NOT NULL,
  schemaInstanceId BIGINT                NOT NULL,
  timestamp        BIGINT                NOT NULL,
  name             VARCHAR(255)          NOT NULL,
  fieldNamespace   VARCHAR(255),
  type             VARCHAR(255)          NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (schemaInstanceId) REFERENCES schema_version_info (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS schema_serdes_info (
  id                    BIGINT AUTO_INCREMENT NOT NULL,
  description           TEXT,
  name                  TEXT                  NOT NULL,
  fileId                TEXT                  NOT NULL,
  serializerClassName   TEXT                  NOT NULL,
  deserializerClassName TEXT                  NOT NULL,
  timestamp             BIGINT                NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS schema_serdes_mapping (
  schemaMetadataId BIGINT NOT NULL,
  serDesId         BIGINT NOT NULL,

  UNIQUE KEY `UK_IDS` (schemaMetadataId, serdesId)
);

-- 3.0.0 doesn't have "validationLevel" column for "schema_metadata_info", handling that case with stored procedure

DROP PROCEDURE IF EXISTS add_validation_level_if_missing;

delimiter ///

CREATE PROCEDURE add_validation_level_if_missing()
BEGIN
    SELECT COUNT(*) INTO @col_exists FROM information_schema.columns WHERE table_schema IN (SELECT DATABASE() FROM DUAL) AND table_name = 'schema_metadata_info' AND column_name = 'validationLevel';
    IF @col_exists = 0 THEN
       SET @str = 'ALTER TABLE schema_metadata_info ADD validationLevel VARCHAR(255) NOT NULL DEFAULT "ALL" AFTER compatibility';
       PREPARE stmt FROM @str;
       EXECUTE stmt;
       DEALLOCATE PREPARE stmt;
    END IF;
END ///

delimiter ;

CALL add_validation_level_if_missing();