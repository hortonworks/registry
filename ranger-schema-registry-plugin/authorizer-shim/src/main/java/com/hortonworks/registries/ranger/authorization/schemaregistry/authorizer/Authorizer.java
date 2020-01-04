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

    boolean authorize(Resource resource, AccessType accessType, String uName, Set<String> uGroup);


    enum AccessType {
        CREATE("create"),
        READ("read"),
        UPDATE("update"),
        DELETE("delete");

        private final String name;

        AccessType(String name) { this.name = name; }

        public String getName() { return name; }
    }

    enum ResourceType {
        SERDE("serde"),
        SCHEMA_METADATA("schema-metadata"),
        SCHEMA_BRANCH("schema-branch"),
        SCHEMA_VERSION("schema-version");

        private final String resourceName;

        ResourceType(String name) { this.resourceName = name; }

        public String getResourceName() { return resourceName; }
    }

    abstract class Resource {
        private ResourceType resourceType;

        public Resource(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        public String getResourceName() {
            return resourceType.getResourceName();
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
            return "SerDeResource{ serDeName='*' }";
        }
    }

    class SchemaMetadataResource extends Resource {
        private String sGroupName;
        private String sMetadataName;

        public SchemaMetadataResource(String sGroupName, String sMetadataName) {
            super(ResourceType.SCHEMA_METADATA);
            this.sGroupName = sGroupName;
            this.sMetadataName = sMetadataName;
        }

        public String getsGroupName() {
            return sGroupName;
        }

        public String getsMetadataName() {
            return sMetadataName;
        }

        @Override
        public String toString() {
            return String.format("SchemaMetadataResource{ schemaGroupName='%s', schemaMetadataName='%s' }",
                    sGroupName, sMetadataName);
        }
    }

    class SchemaBranchResource extends Resource {
        private SchemaMetadataResource schemaMetadataResource;
        private String sBranchName;

        public SchemaBranchResource(SchemaMetadataResource schemaMetadataResource, String sBranchName) {
            super(ResourceType.SCHEMA_BRANCH);
            this.schemaMetadataResource = schemaMetadataResource;
            this.sBranchName = sBranchName;
        }

        public SchemaBranchResource(String sGroupName, String sMetadataName, String sBranchName) {
            this(new SchemaMetadataResource(sGroupName, sMetadataName), sBranchName);
        }

        public SchemaMetadataResource getSchemaMetadataResource() {
            return schemaMetadataResource;
        }

        public String getsBranchName() {
            return sBranchName;
        }

        @Override
        public String toString() {
            return String.format("SchemaBranchResource{ schemaGroupName='%s', schemaMetadataName='%s' schemaBranchName='%s' }",
                    schemaMetadataResource.getsGroupName(),
                    schemaMetadataResource.getsMetadataName(),
                    sBranchName);
        }
    }

    class SchemaVersionResource extends Resource {
        private SchemaBranchResource schemaBranchResource;

        public SchemaVersionResource(SchemaBranchResource schemaBranchResource) {
            super(ResourceType.SCHEMA_VERSION);
            this.schemaBranchResource = schemaBranchResource;
        }

        public SchemaVersionResource(String sGroupName, String sMetadataName, String sBranchName) {
            this(new SchemaBranchResource(sGroupName, sMetadataName, sBranchName));
        }

        public SchemaBranchResource getSchemaBranchResource() {
            return schemaBranchResource;
        }

        @Override
        public String toString() {
            SchemaMetadataResource schemaMetadataResource = schemaBranchResource.getSchemaMetadataResource();
            return String.format("SchemaVersionResource{ schemaGroupName='%s', schemaMetadataName='%s' schemaBranchName='%s' schemaVersionName='*' }",
                    schemaMetadataResource.getsGroupName(),
                    schemaMetadataResource.getsMetadataName(),
                    schemaBranchResource.getsBranchName());
        }
    }

}
