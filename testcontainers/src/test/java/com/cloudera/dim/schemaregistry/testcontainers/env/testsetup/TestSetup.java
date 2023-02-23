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

package com.cloudera.dim.schemaregistry.testcontainers.env.testsetup;

import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.AtlasSetup;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.DbSetup;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.KerberosSetup;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.OAuthSetup;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.TlsSetup;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

@Builder
@Getter
public class TestSetup {

    private static final boolean USE_SR_SERVER_FROM_CONTAINER = false;
    public static final boolean REBUILD_KDC_SERVER_IMAGE_FROM_DOCKERFILE = true;


    public static final boolean SR_USE_FIXED_PORT__DEV_ONLY = false;
    public static final boolean KDC_USE_FIXED_PORT__DEV_ONLY = false;
    public static final boolean DB_USE_FIXED_HOST_PORT__DEV_ONLY = false;


    public static final int SR_FIXED_DEBUG_PORT__DEV_ONLY = 15006;
    public static final int POSTGRES_FIXED_HOST_PORT__DEV_ONLY = 15432;
    public static final int MYSQL_FIXED_HOST_PORT__DEV_ONLY = 13306;
    public static final int KDC_FIXED_PORT__DEV_ONLY = 10088;

    @NonNull
    private DbSetup dbSetup;

    private TlsSetup tlsSetup;

    private KerberosSetup kerberosSetup;

    private OAuthSetup oAuthSetup;
    private AtlasSetup atlasSetup;

    @Builder.Default
    private SchemaCompatibility defaultAvroSchemaCompatibility = SchemaCompatibility.DEFAULT_COMPATIBILITY;

    @Builder.Default
    private SchemaCompatibility defaultJsonSchemaCompatibility = SchemaCompatibility.DEFAULT_COMPATIBILITY;

    @Builder.Default
    private List<ServletFilterConfiguration> additionalFilters = new ArrayList<>();

    @Builder.Default
    private boolean isUseSrServerFromContainer = USE_SR_SERVER_FROM_CONTAINER;

    @NonNull
    private String tempFolderPath;

    public boolean isTlsEnabled() {
        return tlsSetup != null;
    }

    public boolean isKerberosEnabled() {
        return kerberosSetup != null;
    }

    public boolean isOAuthEnabled() {
        return oAuthSetup != null;
    }

    public boolean isAtlasEnabled() {
        return atlasSetup != null;
    }

    public boolean isMTlsEnabled() {
        if (!isTlsEnabled()) {
            return false;
        }
        return tlsSetup.getClientAuthRequired();
    }

}
