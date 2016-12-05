-- CREATE DATABASE IF NOT EXISTS schema_registry;
USE schema_registry;

-- THE NAMES OF THE TABLE COLUMNS MUST MATCH THE NAMES OF THE CORRESPONDING CLASS MODEL FIELDS

CREATE TABLE IF NOT EXISTS schema_metadata_info (
  id            BIGINT AUTO_INCREMENT NOT NULL,
  type          VARCHAR(256)          NOT NULL,
  schemaGroup   VARCHAR(256)          NOT NULL,
  name          VARCHAR(256)          NOT NULL,
  compatibility VARCHAR(256)          NOT NULL,
  description   TEXT,
  timestamp     BIGINT                NOT NULL,
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
  name             VARCHAR(256)          NOT NULL,
  UNIQUE KEY (id),
  UNIQUE KEY `UK_METADATA_ID_VERSION_FK` (schemaMetadataId, version),
  PRIMARY KEY (name, version),
  FOREIGN KEY (schemaMetadataId, name) REFERENCES schema_metadata_info (id, name)
);

CREATE TABLE IF NOT EXISTS schema_field_info (
  id               BIGINT AUTO_INCREMENT NOT NULL,
  schemaInstanceId BIGINT                NOT NULL,
  timestamp        BIGINT                NOT NULL,
  name             VARCHAR(256)          NOT NULL,
  fieldNamespace   VARCHAR(256),
  type             VARCHAR(256)          NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (schemaInstanceId) REFERENCES schema_version_info (id)
);

CREATE TABLE IF NOT EXISTS schema_serdes_info (
  id           BIGINT AUTO_INCREMENT NOT NULL,
  description  TEXT,
  name         TEXT                  NOT NULL,
  fileId       TEXT                  NOT NULL,
  className    TEXT                  NOT NULL,
  isSerializer BOOLEAN               NOT NULL,
  timestamp    BIGINT                NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS schema_serdes_mapping (
  schemaMetadataId BIGINT NOT NULL,
  serDesId         BIGINT NOT NULL,

  UNIQUE KEY `UK_IDS` (schemaMetadataId, serdesId)
);
