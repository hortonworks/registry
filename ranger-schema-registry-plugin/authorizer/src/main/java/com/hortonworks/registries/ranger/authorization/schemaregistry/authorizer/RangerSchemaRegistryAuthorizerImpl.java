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

import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Set;


public class RangerSchemaRegistryAuthorizerImpl implements Authorizer {

    private final RangerBasePlugin plg;

    public RangerSchemaRegistryAuthorizerImpl() {
        this.plg = SchemaRegistryRangerPlugin.getInstance();
    }

    @Override
    public boolean authorizeSerDe(String accessType,
                                  String uName,
                                  Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SERDE, "ANY");

        return authorize(resource, accessType, uName, uGroup)
                || authorizeSRresource(accessType, uName, uGroup);
    }

    @Override
    public boolean authorizeSchemaGroup(String sGroupName,
                                        String accessType,
                                        String uName,
                                        Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_NONE_SCHEMA_METADATA, "ANY");

        return authorize(resource, accessType, uName, uGroup)
                || authorizeSRresource(accessType, uName, uGroup);
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
        resource.setValue(RESOURCE_NONE_SCHEMA_BRANCH, "ANY");

        return authorize(resource, accessType, uName, uGroup)
                || authorizeSRresource(accessType, uName, uGroup);
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
        resource.setValue(RESOURCE_NONE_SCHEMA_VERSION, "ANY");

        return authorize(resource, accessType, uName, uGroup)
                || authorizeSRresource(accessType, uName, uGroup);
    }

    @Override
    public boolean authorizeSchemaVersion(String sGroupName,
                             String sMetadataName,
                             String sBranchName,
                             String accessType,
                             String uName,
                             Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_SCHEMA_GROUP, sGroupName);
        resource.setValue(RESOURCE_SCHEMA_METADATA, sMetadataName);
        resource.setValue(RESOURCE_SCHEMA_BRANCH, sBranchName);
        resource.setValue(RESOURCE_SCHEMA_VERSION, "ANY");

        return authorize(resource, accessType, uName, uGroup)
                || authorizeSRresource(accessType, uName, uGroup);
    }

    private boolean authorizeSRresource(String accessType, String uName, Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RESOURCE_REGISTRY_SERVICE, "ANY");

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
}
