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
package com.hortonworks.registries.schemaregistry.authorizer.core;

import java.util.Map;
import java.util.Set;

public interface Authorizer {

    void configure(Map<String, Object> props);

    boolean authorize(Resource resource, AccessType accessType, UserAndGroups userAndGroups);


    enum AccessType {
        CREATE("create"),
        READ("read"),
        UPDATE("update"),
        DELETE("delete");

        private final String name;

        AccessType(String name) { this.name = name; }

        public String getName() { return name; }
    }

    class UserAndGroups {
        private String user;
        private Set<String> groups;

        public UserAndGroups(String user, Set<String> groups) {
            this.user = user;
            this.groups = groups;
        }

        public String getUser() {
            return user;
        }

        public Set<String> getGroups() {
            return groups;
        }
    }

    enum ResourceType {
        SERDE,
        SCHEMA_METADATA,
        SCHEMA_BRANCH,
        SCHEMA_VERSION;
    }

    abstract class Resource {
        private ResourceType resourceType;

        public Resource(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        public ResourceType getResourceType() {
            return resourceType;
        }
    }

    class SerdeResource extends Resource {
        public SerdeResource() {
            super(ResourceType.SERDE);
        }

        @Override
        public String toString() {
            return "SerDe{ serDeName='*' }";
        }
    }

    class SchemaMetadataResource extends Resource {
        private String sGroupName;
        private String sMetadataName;

        protected SchemaMetadataResource(ResourceType resourceType, String sGroupName, String sMetadataName) {
            super(resourceType);
            this.sGroupName = sGroupName;
            this.sMetadataName = sMetadataName;
        }

        public SchemaMetadataResource(String sGroupName, String sMetadataName) {
            this(ResourceType.SCHEMA_METADATA, sGroupName, sMetadataName);
        }

        public String getsGroupName() {
            return sGroupName;
        }

        public String getsMetadataName() {
            return sMetadataName;
        }

        @Override
        public String toString() {
            return String.format("SchemaMetadata{ schemaGroupName='%s', schemaMetadataName='%s' }",
                    sGroupName, sMetadataName);
        }
    }

    class SchemaBranchResource extends SchemaMetadataResource {
        private String sBranchName;

        protected SchemaBranchResource(ResourceType resourceType, String sGroupName, String sMetadataName, String sBranchName) {
            super(resourceType, sGroupName, sMetadataName);
            this.sBranchName = sBranchName;
        }

        public SchemaBranchResource(String sGroupName, String sMetadataName, String sBranchName) {
            this(ResourceType.SCHEMA_BRANCH, sGroupName, sMetadataName, sBranchName);
        }

        public String getsBranchName() {
            return sBranchName;
        }

        @Override
        public String toString() {
            return String.format("SchemaBranch{ schemaGroupName='%s', schemaMetadataName='%s', schemaBranchName='%s' }",
                    getsGroupName(),
                    getsMetadataName(),
                    sBranchName);
        }
    }

    class SchemaVersionResource extends SchemaBranchResource {

        public SchemaVersionResource(String sGroupName, String sMetadataName, String sBranchName) {
            super(ResourceType.SCHEMA_VERSION, sGroupName, sMetadataName, sBranchName);
        }

        @Override
        public String toString() {
            return String.format("SchemaVersion{ schemaGroupName='%s', schemaMetadataName='%s', schemaBranchName='%s', schemaVersionName='*' }",
                    getsGroupName(),
                    getsMetadataName(),
                    getsBranchName());
        }
    }

}
