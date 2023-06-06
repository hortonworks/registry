-- Copyright 2018-2023 Cloudera, Inc.;
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

LOCK TABLES schema_version_info WRITE, schema_version_state WRITE;

DELETE FROM schema_version_state WHERE NOT EXISTS(SELECT 1 FROM schema_version_info WHERE schema_version_state.schemaVersionId = schema_version_info.id);

ALTER TABLE schema_version_state ADD CONSTRAINT schema_version_state_FK FOREIGN KEY (schemaVersionId) REFERENCES schema_version_info(id) ON DELETE CASCADE;

UNLOCK TABLES;
