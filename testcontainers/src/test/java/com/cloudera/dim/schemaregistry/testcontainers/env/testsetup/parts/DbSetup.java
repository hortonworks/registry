/**
 * Copyright 2016-2023 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts;

import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbType;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import static com.cloudera.dim.schemaregistry.testcontainers.env.envutils.testcontainers.TestcontainersUtils.MYSQL_PORT_INSIDE_CONTAINER;
import static com.cloudera.dim.schemaregistry.testcontainers.env.envutils.testcontainers.TestcontainersUtils.POSTGRES_PORT_INSIDE_CONTAINER;
import static com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup.DB_USE_FIXED_HOST_PORT__DEV_ONLY;
import static com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup.MYSQL_FIXED_HOST_PORT__DEV_ONLY;
import static com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup.POSTGRES_FIXED_HOST_PORT__DEV_ONLY;


@Builder
public class DbSetup {

    private static final String DB_CONN_USER_PASS = "sr_pass";
    private static final String DB_CONN_USER_NAME = "sr_user";
    private static final String DB_NAME = "sr_db";

    @NonNull
    @Getter
    private DbType usedDbType;
    @Getter
    @Builder.Default
    private String dbName = DB_NAME;
    @Getter
    @Builder.Default
    private String dbUserName = DB_CONN_USER_NAME;
    @Getter
    @Builder.Default
    private String dbUserPass = DB_CONN_USER_PASS;

    private Integer dbPortInsideContainer;
    private Integer dbPortFixedForDebugging;

    @Getter
    @Builder.Default
    private Boolean isFixedDbPortUsed = DB_USE_FIXED_HOST_PORT__DEV_ONLY;

    public int getDbPortInsideContainer() {
        if (dbPortInsideContainer == null) {
            switch (usedDbType) {
                case H2:
                    // H2 runs in memory
                    return 0;
                case MYSQL5:
                case MYSQL8:
                    return MYSQL_PORT_INSIDE_CONTAINER;
                case POSTGRES:
                    return POSTGRES_PORT_INSIDE_CONTAINER;
                default:
                    throw new UnsupportedOperationException("unknown db type");
            }
        }
        return dbPortInsideContainer.intValue();
    }

    public int getDbPortFixedForDebugging() {
        if (dbPortFixedForDebugging == null) {
            switch (usedDbType) {
                case H2:
                    // not implemented yet to specify port for in-base H2 DB connection
                    return 0;
                case MYSQL5:
                case MYSQL8:
                    return MYSQL_FIXED_HOST_PORT__DEV_ONLY;
                case POSTGRES:
                    return POSTGRES_FIXED_HOST_PORT__DEV_ONLY;
                default:
                    throw new UnsupportedOperationException("unknown db type");
            }
        }
        return dbPortFixedForDebugging.intValue();
    }
}
