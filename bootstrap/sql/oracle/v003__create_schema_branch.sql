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

CREATE TABLE "schema_branch" (
  "id"                    NUMBER(19,0)          NOT NULL,
  "name"                  VARCHAR2(4000)        NOT NULL,
  "description"           VARCHAR2(4000),
  "timestamp"             NUMBER(19,0),
  CONSTRAINT schema_branch_pk PRIMARY KEY ("name"),
  CONSTRAINT schema_branch_uk UNIQUE ("id")
);

CREATE TABLE "schema_branch_version_mapping" (
  "schemaBranchId"         NUMBER(19,0)          NOT NULL,
  "schemaVersionInfoId"    NUMBER(19,0)          NOT NULL,
  CONSTRAINT schema_branch_version_uk UNIQUE ("schemaBranchId","schemaVersionInfoId"),
  CONSTRAINT schema_branch_version_bid FOREIGN KEY ("schemaBranchId") REFERENCES "schema_branch" ("id") ON DELETE CASCADE,
  CONSTRAINT schema_branch_version_vid FOREIGN KEY ("schemaVersionInfoId") REFERENCES "schema_version_info" ("id") ON DELETE CASCADE
);

CREATE SEQUENCE "SCHEMA_BRANCH" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000;

INSERT INTO "schema_branch" ("id", "name", "description") VALUES ("SCHEMA_BRANCH".NEXTVAL , 'MASTER', 'Schemas in this branch are meant to be consumed for production environment');

CREATE OR REPLACE PROCEDURE update_schema_version_branch AUTHID CURRENT_USER AS
  branch_id  NUMBER;
BEGIN
    SELECT "id" INTO branch_id FROM "schema_branch" WHERE "name" = 'MASTER';

    FOR ptr IN (SELECT "id" FROM "schema_version_info")
    LOOP
        INSERT INTO "schema_branch_version_mapping" VALUES (branch_id, ptr."id");
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END update_schema_version_branch;

/

CALL update_schema_version_branch();
