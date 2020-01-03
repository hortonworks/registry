/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer;


import java.util.Set;

public interface Authorizer {
    String ACCESS_TYPE_CREATE = "create";
    String ACCESS_TYPE_READ = "read";
    String ACCESS_TYPE_UPDATE = "update";
    String ACCESS_TYPE_DELETE = "delete";

    String RESOURCE_REGISTRY_SERVICE = "registry-service";
    String RESOURCE_SERDE = "serde";
    String RESOURCE_SCHEMA_GROUP = "schema-group";
    String RESOURCE_SCHEMA_METADATA = "schema-metadata";
    String RESOURCE_NONE_SCHEMA_METADATA = "none-sm";
    String RESOURCE_SCHEMA_BRANCH = "schema-branch";
    String RESOURCE_NONE_SCHEMA_BRANCH = "none-sb";
    String RESOURCE_SCHEMA_VERSION = "schema-version";
    String RESOURCE_NONE_SCHEMA_VERSION = "none-sv";

    boolean authorizeSerDe(String accessType,
                           String uName,
                           Set<String> uGroup);

    boolean authorizeSchemaGroup(String sGroupName,
                                 String accessType,
                                 String uName,
                                 Set<String> uGroup);

    boolean authorizeSchema(String sGroupName,
                            String sMetadataName,
                            String accessType,
                            String uName,
                            Set<String> uGroup);

    boolean authorizeSchemaBranch(String sGroupName,
                                  String sMetadataName,
                                  String sBranchName,
                                  String accessType,
                                  String uName,
                                  Set<String> uGroup);

    boolean authorizeSchemaVersion(String sGroupName,
                                   String sMetadataName,
                                   String sBranchName,
                                   String accessType,
                                   String uName,
                                   Set<String> uGroup);
}
