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

CREATE TABLE IF NOT EXISTS "schema_branch" (
  "id"          SERIAL          UNIQUE NOT NULL,
  "name"        VARCHAR(255)    NOT NULL,
  "description" TEXT,
  "timestamp"   BIGINT          DEFAULT NULL,
  PRIMARY KEY ("name"),
  UNIQUE ("id")
);

CREATE TABLE IF NOT EXISTS "schema_branch_version_mapping" (
  "schemaBranchId"         BIGINT     NOT NULL,
  "schemaVersionInfoId"    BIGINT     NOT NULL,
  UNIQUE ("schemaBranchId","schemaVersionInfoId"),
  FOREIGN KEY ("schemaBranchId") REFERENCES "schema_branch" (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY ("schemaVersionInfoId") REFERENCES "schema_version_info" (id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO "schema_branch" ("name","description") VALUES ('MASTER', 'Schemas in this branch are meant to be consumed for production environment');

CREATE OR REPLACE FUNCTION update_schema_version_branch()
  RETURNS void AS $$
  DECLARE
      branch_id  BIGINT;
      id_cursor CURSOR FOR
         SELECT "id" FROM "schema_version_info";
  BEGIN
    SELECT "id" INTO branch_id FROM "schema_branch" WHERE "name" = 'MASTER';
    FOR ptr IN id_cursor LOOP
        INSERT INTO "schema_branch_version_mapping" VALUES (branch_id, ptr.id);
    END LOOP;
  END;
$$ LANGUAGE plpgsql;

SELECT update_schema_version_branch();
