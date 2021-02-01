/**
 * Copyright 2017-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


package com.hortonworks.registries.storage.tool.shell;

import com.hortonworks.registries.storage.tool.sql.StorageProviderConfiguration;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;

import java.nio.charset.StandardCharsets;

public class ShellFlywayFactory {

    private static final String ENCODING = StandardCharsets.UTF_8.name();
    private static final String META_DATA_TABLE_NAME = "SCRIPT_CHANGE_LOG";
    private static final String SHELL_MIGRATION_PREFIX = "v";
    private static final String SHELL_MIGRATION_SUFFIX = ".sh";
    private static final String SHELL_MIGRATION_SEPERATOR = "__";
    private static final boolean VALIDATE_ON_MIGRATE = true;
    private static final boolean OUT_OF_ORDER = false;
    private static final boolean BASELINE_ON_MIGRATE = true;
    private static final String BASELINE_VERSION = "000";
    private static final boolean CLEAN_ON_VALIDATION_ERROR = false;


    public static Flyway get(StorageProviderConfiguration conf, String scriptRootPath) {
        Flyway flyway = new Flyway();

        String location = "filesystem:" + scriptRootPath;
        flyway.setEncoding(ENCODING);
        flyway.setTable(META_DATA_TABLE_NAME);
        flyway.setValidateOnMigrate(VALIDATE_ON_MIGRATE);
        flyway.setOutOfOrder(OUT_OF_ORDER);
        flyway.setBaselineOnMigrate(BASELINE_ON_MIGRATE);
        flyway.setBaselineVersion(MigrationVersion.fromVersion(BASELINE_VERSION));
        flyway.setCleanOnValidationError(CLEAN_ON_VALIDATION_ERROR);
        flyway.setLocations(location);
        flyway.setResolvers(new ShellMigrationResolver(flyway.getConfiguration(), location,
                SHELL_MIGRATION_PREFIX, SHELL_MIGRATION_SEPERATOR, SHELL_MIGRATION_SUFFIX));
        flyway.setDataSource(conf.getUrl(), conf.getUser(), conf.getPassword(), null);

        return flyway;
    }

}
