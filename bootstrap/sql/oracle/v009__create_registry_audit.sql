-- Copyright 2018-2021 Cloudera, Inc.;
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

CALL create_if_not_exists('TABLE', 'CREATE TABLE "atlas_events" (
  "id"              NUMBER(19,0)        NOT NULL,
  "username"        VARCHAR2(255)       NOT NULL,
  "processedId"     NUMBER(19,0)        NOT NULL,
  "type"            NUMBER(8,0)         NOT NULL,
  "processed"       NUMBER(1)           NOT NULL,
  "failed"          NUMBER(1)           NOT NULL,
  "timestamp"       NUMBER(19,0)        NOT NULL,
  CONSTRAINT atlas_events_pk PRIMARY KEY ("id")
)');

CALL create_if_not_exists('SEQUENCE', 'CREATE SEQUENCE "ATLAS_EVENTS" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000');

DECLARE index_count INTEGER;
BEGIN
SELECT COUNT(*) INTO index_count
    FROM USER_INDEXES
    WHERE INDEX_NAME = 'atlas_events_processed';

IF index_count > 0 THEN
    EXECUTE IMMEDIATE 'DROP INDEX atlas_events_processed';
END IF;
END;
/

CREATE INDEX atlas_events_processed ON "atlas_events"("processed");