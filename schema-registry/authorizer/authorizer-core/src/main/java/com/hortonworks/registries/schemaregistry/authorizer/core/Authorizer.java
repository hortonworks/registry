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

/**
 * This is an interface that is implemented by RangerSchemaRegistryAuthorizer
 * to provide autorization by means of Apache Ranger.
 * The user can define its own authorizer by implementing the authorize and configure methods.
 * The exact interface type that will be instatiated is specified by AUTHORIZER_CONFIG property.
 */
public interface Authorizer {

    /**
     * Schema Registry config property name that is used to specify Authorizer class name.
     */
    String AUTHORIZER_CONFIG = "authorizerClassName";

    /**
     * This method is used to perform initial configuration of authorizer.
     *
     * @param props - properties from the 'authorization' section of Schema Registry config file.
     */
    void configure(Map<String, Object> props);

    /**
     * The main method that performs the authorization
     *
     * @param resource - an object to check access to
     * @param accessType - required access type
     * @param userAndGroups - user and user groups that are use for permission check
     * @return 'true' if access is allowed and 'false' otherwise
     */
    boolean authorize(Resource resource, AccessType accessType, UserAndGroups userAndGroups);


    ///// The below classes are used to define independent object model for authorization /////

    /**
     * The 'AccessType' enum specifies supported access types by current authorization model
     */
    enum AccessType {
        CREATE("create"),
        READ("read"),
        UPDATE("update"),
        DELETE("delete");

        private final String name;

        AccessType(String name) { this.name = name; }

        public String getName() { return name; }
    }

    /**
     * The class that is used to store user and user groups information in convenient form.
     */
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

        @Override
        public String toString() {
            return String.format("UserAndGroups{ user='%s', groups='%s' }", user, groups);
        }
    }

    /**
     * The 'ResourceType' enum specifies supported types of resources by current authorization model
     */
    enum ResourceType {
        SERDE,
        SCHEMA_METADATA,
        SCHEMA_BRANCH,
        SCHEMA_VERSION;
    }

    /**
     * Class that is used to represent abstarct resource entity
     */
    abstract class Resource {
        private ResourceType resourceType;

        public Resource(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        public ResourceType getResourceType() {
            return resourceType;
        }
    }

    /**
     * Class that is used to represent Serializer/Deserializer resources
     */
    class SerdeResource extends Resource {
        public SerdeResource() {
            super(ResourceType.SERDE);
        }

        @Override
        public String toString() {
            return "SerDe{ serDeName='*' }";
        }
    }

    /**
     * Class that is used to represent schema metadata resource
     */
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

    /**
     * Class that is used to represent schema branch resource
     */
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

    /**
     * Class that is used to represent schema version resource
     */
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
