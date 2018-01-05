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
  "id"                   SERIAL          UNIQUE NOT NULL,
  "name"                 VARCHAR(255)    NOT NULL,
  "schemaMetadataName"   VARCHAR(255)    NOT NULL,
  "description"          TEXT,
  "timestamp"            BIGINT          DEFAULT NULL,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("schemaMetadataName") REFERENCES "schema_metadata_info" ("name") ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE ("name", "schemaMetadataName")
);

CREATE TABLE IF NOT EXISTS "schema_branch_version_mapping" (
  "schemaBranchId"         BIGINT     NOT NULL,
  "schemaVersionInfoId"    BIGINT     NOT NULL,
  UNIQUE ("schemaBranchId","schemaVersionInfoId"),
  FOREIGN KEY ("schemaBranchId") REFERENCES "schema_branch" (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY ("schemaVersionInfoId") REFERENCES "schema_version_info" (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE OR REPLACE FUNCTION update_schema_version_branch()
  RETURNS void AS $$
  DECLARE
      branch_id  BIGINT;
      branch_desc TEXT;
      metadata_cursor CURSOR FOR
         SELECT "name" FROM "schema_metadata_info";
      id_cursor CURSOR FOR
         SELECT "id","name" FROM "schema_version_info";
  BEGIN
    FOR meta_ptr IN metadata_cursor LOOP
        branch_desc := '''MASTER'' branch for schema metadata ''' || meta_ptr."name" || '''' ;
        INSERT INTO "schema_branch" ("name", "schemaMetadataName", "description") VALUES ('MASTER', meta_ptr."name", branch_desc);
    END LOOP;

    FOR ptr IN id_cursor LOOP
        SELECT "id" INTO branch_id FROM "schema_branch" WHERE "name" = 'MASTER' AND "schemaMetadataName" = ptr."name";
        INSERT INTO "schema_branch_version_mapping" VALUES (branch_id, ptr.id);
    END LOOP;
  END;
$$ LANGUAGE plpgsql;

SELECT update_schema_version_branch();
