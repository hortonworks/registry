/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.cloudera.dim.schemaregistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalState {

    public static final String SCHEMA_NAME = "schemaName";
    public static final String SCHEMA_ID = "schemaId";
    public static final String SCHEMA_META_INFO = "schemaMetaInfo";
    public static final String SCHEMA_VERSION_ID = "schemaVersionId";
    public static final String SCHEMA_VERSION_NO = "schemaVersionNo";
    public static final String SCHEMA_VERSION_TEXT = "schemaVersionText";
    public static final String AGGREGATED_SCHEMAS = "aggregatedSchemas";
    public static final String COMPATIBILITY = "compatibility";
    public static final String HTTP_RESPONSE_CODE = "httpResponseCode";
    public static final String EXCEPTION_MSG = "exceptionMsg";
    public static final String ATLAS_ENTITIES = "atlasEntities";
    public static final String ATLAS_RELATIONSHIPS = "atlasRelationships";
    public static final String ATLAS_ENTITY_UPDATE = "atlasEntityUpdate";
    public static final String SCHEMA_EXPORT = "schemaExport";
    public static final String AUTH_TOKEN = "authToken";
    public static final String RESPONSE_IN_JSON = "responseJson";

    private final Map<String, Object> sow = new ConcurrentHashMap<>();

    private volatile static GlobalState instance;

    public static GlobalState getInstance() {
        GlobalState localRef = instance;
        if (localRef == null) {
            synchronized (GlobalState.class) {
                if (localRef == null) {
                    instance = localRef = new GlobalState();
                }
            }
        }
        return localRef;
    }

    private GlobalState() {
    }

    public Map<String, Object> getSow() {
        return sow;
    }

    public Object getValue(String key) {
        return sow.get(key);
    }

    public void setValue(String key, Object value) {
        sow.put(key, value);
    }

    public void clear() {
        sow.clear();
    }

}
