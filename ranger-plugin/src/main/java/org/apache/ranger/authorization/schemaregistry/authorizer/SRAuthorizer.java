package org.apache.ranger.authorization.schemaregistry.authorizer;

import com.hortonworks.registries.schemaregistry.*;

public interface SRAuthorizer {
    String ACCESS_TYPE_CREATE = "create";
    String ACCESS_TYPE_READ = "read";
    String ACCESS_TYPE_UPDATE = "update";
    String ACCESS_TYPE_DELETE = "delete";

    String RESOURCE_FILE = "file";
    String RESOURCE_SERDE = "serde";
    String RESOURCE_SCHEMA_GROUP = "schema-group";
    String RESOURCE_SCHEMA_METADATA = "schema-metadata";
    String RESOURCE_SCHEMA_BRANCH = "schema-branch";
    String RESOURCE_SCHEMA_VERSION = "schema-version";

    boolean authorizeFile(int fileId, String accessType);
    boolean authorizeSerDe(String sName, String accessType);
    boolean authorizeSchemaGroup(String sGroupName, String accessType);
    boolean authorizeSchema(String sGroupName, String sMetadataName, String accessType);
    boolean authorizeSchemaBranch(String sGroupName,
                                  String sMetadataName,
                                  String sBranchName,
                                  String accessType);
    boolean authorizeSchemaVersion(String sGroupName,
                                   String sMetadataName,
                                   String sBranchName,
                                   String sVersion,
                                   String accessType);
}
