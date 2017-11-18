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

CREATE TABLE schema_branch (
  `id`          BIGINT(20)      NOT NULL AUTO_INCREMENT,
  `name`        VARCHAR(255)    NOT NULL,
  `description` TEXT,
  `timestamp`   BIGINT(20)      DEFAULT NULL,
  PRIMARY KEY (`name`),
  UNIQUE KEY `UK_SCHEMA_BRANCH_ID` (`id`)
);


CREATE TABLE schema_branch_version_mapping (
  `schemaBranchId`         BIGINT(20)     NOT NULL,
  `schemaVersionInfoId`    BIGINT(20)     NOT NULL,
  UNIQUE KEY `UK_SCHEMA_BRANCH_VERSION` (`schemaBranchId`,`schemaVersionInfoId`),
  FOREIGN KEY (schemaBranchId) REFERENCES `schema_branch` (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (schemaVersionInfoId) REFERENCES `schema_version_info` (id) ON DELETE CASCADE ON UPDATE CASCADE
);


INSERT INTO schema_branch (`name`,`description`) VALUES ('MASTER', 'Schemas in this branch are meant to be consumed for production environment');

DROP PROCEDURE IF EXISTS update_schema_version_with_branch;

DELIMITER ///

CREATE PROCEDURE update_schema_version_with_branch()
BEGIN
    DECLARE schema_version_cursor_id BIGINT;
    DECLARE done INT DEFAULT FALSE;
    DECLARE schema_version_cursor CURSOR FOR SELECT id FROM schema_version_info;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    DECLARE exit handler for sqlexception
        BEGIN
            ROLLBACK;
            RESIGNAL;
        END;

    START TRANSACTION;

    SELECT id INTO @master_branch_id FROM schema_branch WHERE `name` = "MASTER";
    OPEN schema_version_cursor;
    read_loop: LOOP
        FETCH schema_version_cursor INTO schema_version_cursor_id;
        IF done THEN
            LEAVE read_loop;
        END IF;
        INSERT INTO schema_branch_version_mapping values(@master_branch_id,schema_version_cursor_id);
    END LOOP;
    CLOSE schema_version_cursor;

    COMMIT;
END ///

DELIMITER ;

CALL update_schema_version_with_branch();










