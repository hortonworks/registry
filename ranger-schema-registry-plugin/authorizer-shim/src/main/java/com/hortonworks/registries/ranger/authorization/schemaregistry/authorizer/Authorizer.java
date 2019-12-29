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
    String RESOURCE_SCHEMA_BRANCH = "schema-branch";
    String RESOURCE_SCHEMA_VERSION = "schema-version";

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
