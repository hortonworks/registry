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

CREATE TABLE "registry_audit" (
  "id"              NUMBER(19,0)        NOT NULL,
  "username"        VARCHAR2(255)       NOT NULL,
  "processedData"  VARCHAR2(4000),
  "processed"       NUMBER(1)           NOT NULL,
  "failed"          NUMBER(1)           NOT NULL,
  "timestamp"       NUMBER(19,0)        NOT NULL,
  CONSTRAINT registry_audit_pk PRIMARY KEY ("id")
);

CREATE SEQUENCE "REGISTRY_AUDIT" START WITH 1 INCREMENT BY 1 MAXVALUE 10000000000000000000;
CREATE INDEX registry_audit_processed ON registry_audit("processed");
