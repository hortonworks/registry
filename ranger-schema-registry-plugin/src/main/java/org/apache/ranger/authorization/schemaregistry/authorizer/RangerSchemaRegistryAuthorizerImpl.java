package org.apache.ranger.authorization.schemaregistry.authorizer;

import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class RangerSchemaRegistryAuthorizerImpl implements Authorizer {

    private final RangerBasePlugin plg;

    public RangerSchemaRegistryAuthorizerImpl() {
        this.plg = SchemaRegistryRangerPlugin.getInstance();
    }

    @Override
    public boolean authorizeSerDe(String sName,
                                  String accessType,
                                  String uName,
                                  Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SERDE, sName);

        return authorize(resource, accessType, uName, uGroup);
    }

    @Override
    public boolean authorizeSchemaGroup(String sGroupName,
                                        String accessType,
                                        String uName,
                                        Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);

        return authorize(resource, accessType, uName, uGroup);
    }

    @Override
    public boolean authorizeSchema(String sGroupName,
                                   String sMetadataName,
                                   String accessType,
                                   String uName,
                                   Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_SCHEMA_METADATA, sMetadataName);

        return authorize(resource, accessType, uName, uGroup);
    }

    @Override
    public boolean authorizeSchemaBranch(String sGroupName,
                                         String sMetadataName,
                                         String sBranchName,
                                         String accessType,
                                         String uName,
                                         Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_SCHEMA_METADATA, sMetadataName);
        resource.setValue(RESOURCE_SCHEMA_BRANCH, sBranchName);

        return authorize(resource, accessType, uName, uGroup);
    }

    @Override
    public boolean authorizeSchemaVersion(String sGroupName,
                             String sMetadataName,
                             String sBranchName,
                             String sVersion,
                             String accessType,
                             String uName,
                             Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_SCHEMA_METADATA, sMetadataName);
        resource.setValue(RESOURCE_SCHEMA_BRANCH, sBranchName);
        resource.setValue(RESOURCE_SCHEMA_VERSION, sVersion);

        return authorize(resource, accessType, uName, uGroup);
    }

    private boolean authorize(RangerAccessResourceImpl resource,
                              String accessType,
                              String uName,
                              Set<String> uGroup) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, accessType, uName, uGroup);
        RangerAccessResult res = plg.isAccessAllowed(request);

        return res != null && res.getIsAllowed();
    }

    private static class SchemaRegistryRangerPlugin extends RangerBasePlugin {
        private static final String PLG_TYPE = "schema-registry";
        private static final String PLG_NAME = "schema-registry";

        private static SchemaRegistryRangerPlugin instance;

        private SchemaRegistryRangerPlugin() {
            this(PLG_TYPE, PLG_NAME);
        }
        private SchemaRegistryRangerPlugin(String serviceType, String appId) {
            super(serviceType, appId);
        }

        private static SchemaRegistryRangerPlugin getInstance() {
            if (instance == null) {
                synchronized (SchemaRegistryRangerPlugin.class) {
                    if (instance == null) {
                        instance = new SchemaRegistryRangerPlugin();
                        instance.setResultProcessor(new RangerSchemaRegistryAuditHandler());
                        instance.init();
                    }
                }
            }
            return instance;
        }
    }

    public static void main(String[] args) throws IOException {
        String user = "vagrant";
        Set<String> groups = new HashSet<>();
        groups.add("vagrant");
        Authorizer authorizer = new RangerSchemaRegistryAuthorizerImpl();
        String sGroup = "";
        String sMetadataName = "";
        String sBranch = "";
        String sVersion = "";

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("//--- SchemaMetadata read authorization ---");

        sGroup = "Group1";
        sMetadataName = "test1";

        System.out.println("\nUser = " + user);
        System.out.println("UserGroups = " + groups);
        System.out.println("\nSchemaGroup = " + sGroup);
        System.out.println("SchemaMetadata.name = " + sMetadataName);

        boolean isAuthorized = authorizer.authorizeSchema(sGroup, sMetadataName,Authorizer.ACCESS_TYPE_READ,
                user, groups);

        System.out.println("\nAUTHORIZED = " + isAuthorized);

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("\n////--- SchemaBranch read authorization ---");

        sGroup = "Group1";
        sMetadataName = "test1";
        sBranch = "MASTER";

        System.out.println("\nUser = " + user);
        System.out.println("UserGroups = " + groups);
        System.out.println("\nSchemaGroup = " + sGroup);
        System.out.println("SchemaMetadata.name = " + sMetadataName);
        System.out.println("SchemaBranch = " + sBranch);

        isAuthorized = authorizer.authorizeSchemaBranch(sGroup, sMetadataName, sBranch, Authorizer.ACCESS_TYPE_READ,
                user, groups);

        System.out.println("\nAUTHORIZED = " + isAuthorized);

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("\n////////--- SchemaVersion read authorization ---");

        sGroup = "Group1";
        sMetadataName = "test1";
        sBranch = "MASTER1";
        sVersion = "1";

        System.out.println("\nUser = " + user);
        System.out.println("UserGroups = " + groups);
        System.out.println("\nSchemaGroup = " + sGroup);
        System.out.println("SchemaMetadata.name = " + sMetadataName);
        System.out.println("SchemaBranch = " + sBranch);
        System.out.println("SchemaVersion = " + sVersion);

        isAuthorized = authorizer.authorizeSchemaVersion(sGroup, sMetadataName, sBranch, sVersion, Authorizer.ACCESS_TYPE_READ,
                user, groups);

        System.out.println("\nAUTHORIZED = " + isAuthorized);

        System.out.println("\n-----------------------------------------");

    }

}
