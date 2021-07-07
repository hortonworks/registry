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

CREATE TABLE IF NOT EXISTS atlas_events (
  id              BIGINT AUTO_INCREMENT NOT NULL,
  username        VARCHAR(255)          NOT NULL,
  processedId     BIGINT                NOT NULL,
  type            INT                   NOT NULL,
  processed       BOOLEAN               NOT NULL,
  failed          BOOLEAN               NOT NULL,
  timestamp       BIGINT                NOT NULL,
  UNIQUE KEY (id)
);
CREATE INDEX atlas_events_processed ON atlas_events(processed);
