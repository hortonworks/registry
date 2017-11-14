-- Copyright 2017 Hortonworks.;
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


CREATE TABLE "schema_version_state" (
  "id"              NUMBER(19,0)          NOT NULL,
  "schemaVersionId" NUMBER(19,0)          NOT NULL,
  "stateId"         NUMBER(3,0)           NOT NULL,
  "sequence"        NUMBER(10,0)          NOT NULL,
  "timestamp"       NUMBER(19,0)          NOT NULL,
  "details"         BLOB,
  CONSTRAINT schema_version_state_pk PRIMARY KEY ("schemaVersionId", "stateId", "sequence"),
  CONSTRAINT schema_version_state_uk UNIQUE ("id")
);

CREATE SEQUENCE "SCHEMA_VERSION_STATE" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000;

ALTER TABLE "schema_version_info" ADD "state" NUMBER(3,0) DEFAULT 5 NOT NULL;