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

-- This script makes all the necessary changes to bring the database to 3.0.2 from any of the previous versions

-- User should have a explicit create table privileges instead of having said privilege through roles.
-- For example :- The user if he has "resource" role below migration will still fail if the user doesn't have explicit "create table" privilege.

CREATE OR REPLACE PROCEDURE create_if_not_exists( object_type IN VARCHAR2, create_statement IN VARCHAR2 ) AUTHID CURRENT_USER IS
BEGIN
    DBMS_OUTPUT.put_line (create_statement);
    EXECUTE IMMEDIATE create_statement;
EXCEPTION
    WHEN OTHERS THEN
        IF (object_type = 'TABLE' AND SQLCODE != -955) OR (object_type = 'SEQUENCE' AND SQLCODE != -955) THEN
            RAISE;
        END IF;
END create_if_not_exists;

/

CALL create_if_not_exists('TABLE', 'CREATE TABLE "schema_metadata_info" (
  "id"              NUMBER(19,0)           NOT NULL,
  "type"            VARCHAR2(255)          NOT NULL,
  "schemaGroup"     VARCHAR2(255)          NOT NULL,
  "name"            VARCHAR2(255)          NOT NULL,
  "compatibility"   VARCHAR2(255)          NOT NULL,
  "validationLevel" VARCHAR2(255)          NOT NULL,
  "description"     VARCHAR2(4000),
  "evolve"          NUMBER(1)              NOT NULL,
  "timestamp"       NUMBER(19,0)           NOT NULL,
  CONSTRAINT schema_metadata_info_pk PRIMARY KEY ("name"),
  CONSTRAINT schema_metadata_info_uk UNIQUE ("id")
)');

CALL create_if_not_exists('TABLE', 'CREATE TABLE "schema_version_info" (
  "id"               NUMBER(19,0)          NOT NULL,
  "description"      VARCHAR2(4000),
  "schemaText"       CLOB                  NOT NULL,
  "fingerprint"      VARCHAR2(4000)        NOT NULL,
  "version"          NUMBER(10,0)          NOT NULL,
  "schemaMetadataId" NUMBER(19,0)          NOT NULL,
  "timestamp"        NUMBER(19,0)          NOT NULL,
  "name"             VARCHAR2(255)         NOT NULL,
  CONSTRAINT schema_vinfo_uk_id UNIQUE ("id"),
  CONSTRAINT schema_vinfo_uk_mid_v UNIQUE ("schemaMetadataId", "version"),
  CONSTRAINT schema_vinfo_pk PRIMARY KEY ("name", "version"),
  CONSTRAINT schema_vinfo_fk_smi_id FOREIGN KEY ("schemaMetadataId") REFERENCES "schema_metadata_info" ("id") ON DELETE CASCADE,
  CONSTRAINT schema_vinfo_fk_smi_name FOREIGN KEY ("name") REFERENCES "schema_metadata_info" ("name") ON DELETE CASCADE
)');

CALL create_if_not_exists('TABLE', 'CREATE TABLE "schema_field_info" (
  "id"               NUMBER(19,0)          NOT NULL,
  "schemaInstanceId" NUMBER(19,0)          NOT NULL,
  "timestamp"        NUMBER(19,0)          NOT NULL,
  "name"             VARCHAR2(255)         NOT NULL,
  "fieldNamespace"   VARCHAR2(255),
  "type"             VARCHAR2(255)         NOT NULL,
  CONSTRAINT schema_field_info_pk PRIMARY KEY ("id"),
  CONSTRAINT schema_field_info_fk_svid FOREIGN KEY ("schemaInstanceId") REFERENCES "schema_version_info" ("id") ON DELETE CASCADE
)');

CALL create_if_not_exists('TABLE', 'CREATE TABLE "schema_serdes_info" (
  "id"                    NUMBER(19,0)          NOT NULL,
  "description"           VARCHAR2(4000),
  "name"                  VARCHAR2(4000)        NOT NULL,
  "fileId"                VARCHAR2(4000)        NOT NULL,
  "serializerClassName"   VARCHAR2(4000)        NOT NULL,
  "deserializerClassName" VARCHAR2(4000)        NOT NULL,
  "timestamp"             NUMBER(19,0)          NOT NULL,
  CONSTRAINT schema_serdes_info_pk PRIMARY KEY ("id")
)');

CALL create_if_not_exists('TABLE', 'CREATE TABLE "schema_serdes_mapping" (
  "schemaMetadataId" NUMBER(19,0) NOT NULL,
  "serDesId"         NUMBER(19,0) NOT NULL,
  CONSTRAINT schema_serdes_mapping_pk UNIQUE ("schemaMetadataId", "serDesId")
)');


-- User should have CREATE/DROP SEQUENCE privilege to create sequnce which is will be used to get unique id for primary key
--
CALL create_if_not_exists('SEQUENCE', 'CREATE SEQUENCE "SCHEMA_METADATA_INFO" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000');
CALL create_if_not_exists('SEQUENCE', 'CREATE SEQUENCE "SCHEMA_VERSION_INFO" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000');
CALL create_if_not_exists('SEQUENCE', 'CREATE SEQUENCE "SCHEMA_FIELD_INFO" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000');
CALL create_if_not_exists('SEQUENCE', 'CREATE SEQUENCE "SCHEMA_SERDES_INFO" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000');
CALL create_if_not_exists('SEQUENCE', 'CREATE SEQUENCE "SCHEMA_SERDES_MAPPING" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000');