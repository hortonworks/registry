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

-- Change column type from TEXT to MEDIUMTEXT

ALTER TABLE `schema_version_info` MODIFY `schemaText` MEDIUMTEXT NOT NULL;

CREATE TABLE `schema_branch` (
  `id`                   BIGINT(20)      NOT NULL AUTO_INCREMENT,
  `name`                 VARCHAR(255)    NOT NULL,
  `schemaMetadataName`   VARCHAR(255)    NOT NULL,
  `description`          TEXT,
  `timestamp`            BIGINT(20)      DEFAULT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY (`schemaMetadataName`) REFERENCES `schema_metadata_info` (`name`) ON DELETE CASCADE ON UPDATE CASCADE,
  UNIQUE KEY `UK_SCHEMA_BRANCH` (`name`,`schemaMetadataName`)
);


CREATE TABLE schema_branch_version_mapping (
  `schemaBranchId`         BIGINT(20)     NOT NULL,
  `schemaVersionInfoId`    BIGINT(20)     NOT NULL,
  UNIQUE KEY `UK_SCHEMA_BRANCH_VERSION` (`schemaBranchId`,`schemaVersionInfoId`),
  FOREIGN KEY (schemaBranchId) REFERENCES `schema_branch` (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (schemaVersionInfoId) REFERENCES `schema_version_info` (id) ON DELETE CASCADE ON UPDATE CASCADE
);

DROP PROCEDURE IF EXISTS update_schema_version_with_branch;

DELIMITER ///

CREATE PROCEDURE update_schema_version_with_branch()
BEGIN
    DECLARE schema_version_cursor_id BIGINT;
    DECLARE schema_meta_name VARCHAR(255);
    DECLARE done INT DEFAULT FALSE;
    DECLARE schema_version_cursor CURSOR FOR SELECT id, `name` FROM `schema_version_info`;
    DECLARE schema_metadata_cursor CURSOR FOR SELECT `name` FROM `schema_metadata_info`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    DECLARE exit handler for sqlexception
        BEGIN
            ROLLBACK;
            RESIGNAL;
        END;

    START TRANSACTION;

    OPEN schema_metadata_cursor;
    read_loop: LOOP
        FETCH schema_metadata_cursor INTO schema_meta_name;
        IF done THEN
            LEAVE read_loop;
        END IF;
        INSERT INTO `schema_branch` (`name`, `schemaMetadataName`, `description`) VALUES ('MASTER', schema_meta_name, CONCAT ('\'MASTER\' branch for schema metadata \'', schema_meta_name, '\'' ));
    END LOOP;

    SET done = FALSE;

    OPEN schema_version_cursor;
    read_loop: LOOP
        FETCH schema_version_cursor INTO schema_version_cursor_id, schema_meta_name;
        IF done THEN
            LEAVE read_loop;
        END IF;
        SELECT id INTO @master_branch_id FROM schema_branch WHERE `name` = 'MASTER' and `schemaMetadataName` = schema_meta_name;
        INSERT INTO schema_branch_version_mapping VALUES (@master_branch_id, schema_version_cursor_id);
    END LOOP;
    CLOSE schema_version_cursor;

    COMMIT;
END ///

DELIMITER ;

CALL update_schema_version_with_branch();










