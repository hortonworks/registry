package org.apache.ranger.authorization.schemaregistry.authorizer;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class SRDefaultAuthorizer implements SRAuthorizer {

    private static final String PLG_TYPE = "schema-registry";

    private final RangerBasePlugin plg;
    private final String uName;
    private final Set<String> uGroup;

    public SRDefaultAuthorizer(String plgName,
                               String uName,
                               Set<String> uGroup) {
        this.plg = new RangerBasePlugin(PLG_TYPE, plgName);
        this.uName = uName;
        this.uGroup = uGroup;
        plg.setResultProcessor(new RangerSchemaRegistryAuditHandler());
        plg.init();
    }

    @Override
    public boolean authorizeFile(int fileId, String accessType) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_FILE, Integer.toString(fileId));

        return authorize(resource, accessType);
    }

    @Override
    public boolean authorizeSerDe(String sName, String accessType) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SERDE, sName);

        return authorize(resource, accessType);
    }

    @Override
    public boolean authorizeSchemaGroup(String sGroupName, String accessType) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);

        return authorize(resource, accessType);
    }

    @Override
    public boolean authorizeSchema(String sGroupName, String sMetadataName, String accessType) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_SCHEMA_METADATA, sMetadataName);

        return authorize(resource, accessType);
    }

    @Override
    public boolean authorizeSchemaBranch(String sGroupName,
                                         String sMetadataName,
                                         String sBranchName,
                                         String accessType) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_SCHEMA_METADATA, sMetadataName);
        resource.setValue(RESOURCE_SCHEMA_BRANCH, sBranchName);

        return authorize(resource, accessType);
    }

    @Override
    public boolean authorizeSchemaVersion(String sGroupName,
                             String sMetadataName,
                             String sBranchName,
                             String sVersion,
                             String accessType) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_SCHEMA_METADATA, sMetadataName);
        resource.setValue(RESOURCE_SCHEMA_BRANCH, sBranchName);
        resource.setValue(RESOURCE_SCHEMA_VERSION, sVersion);

        return authorize(resource, accessType);
    }

    private boolean authorize(RangerAccessResourceImpl resource, String accessType) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, accessType, uName, uGroup);
        RangerAccessResult res = plg.isAccessAllowed(request);

        return res != null && res.getIsAllowed();
    }

    public static void main(String[] args) throws IOException {
        Set<String> groups = new HashSet<>();
        groups.add("vagrant");
        SRAuthorizer srAuthorizer = new SRDefaultAuthorizer("schema-registy",
                "vagrant", groups);
       boolean isAuthorized = srAuthorizer.authorizeSchemaGroup("Group1", "read");
        isAuthorized = srAuthorizer.authorizeSchema("Group1", "test1","read");

       System.out.println("Authorized=" + isAuthorized);
       System.out.println(UserGroupInformation.getCurrentUser().getGroupNames()[0]);
       System.out.println(UserGroupInformation.getCurrentUser().getUserName());

    }

}
