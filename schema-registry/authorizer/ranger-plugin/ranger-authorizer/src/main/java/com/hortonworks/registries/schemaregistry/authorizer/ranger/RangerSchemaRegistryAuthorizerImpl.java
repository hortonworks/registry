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
package com.hortonworks.registries.schemaregistry.authorizer.ranger;

import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;

import java.util.Set;


public class RangerSchemaRegistryAuthorizerImpl implements Authorizer {

    ///////////// List of Ranger specific resources //////////////////////////

    private static final String RANGER_RESOURCE_REGISTRY_SERVICE = "registry-service";
    private static final String RANGER_RESOURCE_SCHEMA_GROUP = "schema-group";
    private static final String RANGER_RESOURCE_NONE_SCHEMA_BRANCH = "none-sb";
    private static final String RANGER_RESOURCE_NONE_SCHEMA_VERSION = "none-sv";

    private final RangerBasePlugin plg;

    public RangerSchemaRegistryAuthorizerImpl() {
        this.plg = SchemaRegistryRangerPlugin.getInstance();
    }

    @Override
    public boolean authorize(Resource registryResource,
                             AccessType accessType,
                             String uName,
                             Set<String> uGroup) {

        return authorize(registryResource2RangerResources(registryResource), accessType, uName, uGroup)
                || authorizeSRresource(accessType, uName, uGroup);
    }

    private boolean authorizeSRresource(AccessType accessType, String uName, Set<String> uGroup) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RANGER_RESOURCE_REGISTRY_SERVICE, "ANY_VALUE");

        return authorize(resource, accessType, uName, uGroup);
    }

    private boolean authorize(RangerAccessResourceImpl resource,
                              AccessType accessType,
                              String uName,
                              Set<String> uGroup) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, accessType.getName(), uName, uGroup);

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

    private RangerAccessResourceImpl registryResource2RangerResources(Resource resource) {
        switch (resource.getResourceType()) {
            case SERDE: return registryResource2RangerResources((SerdeResource) resource);
            case SCHEMA_METADATA: return registryResource2RangerResources((SchemaMetadataResource) resource);
            case SCHEMA_BRANCH: return registryResource2RangerResources((SchemaBranchResource) resource);
            case SCHEMA_VERSION: return registryResource2RangerResources((SchemaVersionResource) resource);

            // In current implemetataion the exception should never be thrown. This is added for future if
            // the set of resources is extended but implemetation is not provided.
            default: throw new RuntimeException(
                    String.format("Cannot convert registry resource to ranger resources. Resource %s is not supported",
                            resource.getResourceName()));
        }
    }

    private RangerAccessResourceImpl registryResource2RangerResources(SerdeResource serdeResource) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(serdeResource.getResourceName(), "ANY_VALUE");

        return resource;
    }

    private RangerAccessResourceImpl registryResource2RangerResources(SchemaMetadataResource schemaMetadataResource) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RANGER_RESOURCE_SCHEMA_GROUP, schemaMetadataResource.getsGroupName());
        resource.setValue(schemaMetadataResource.getResourceName(), schemaMetadataResource.getsMetadataName());
        resource.setValue(RANGER_RESOURCE_NONE_SCHEMA_BRANCH, "ANY_VALUE");

        return resource;
    }

    private RangerAccessResourceImpl registryResource2RangerResources(SchemaBranchResource schemaBranchResource) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();

        SchemaMetadataResource schemaMetadataResource = schemaBranchResource.getSchemaMetadataResource();

        resource.setValue(RANGER_RESOURCE_SCHEMA_GROUP, schemaMetadataResource.getsGroupName());
        resource.setValue(schemaMetadataResource.getResourceName(), schemaMetadataResource.getsMetadataName());
        resource.setValue(schemaBranchResource.getResourceName(), schemaBranchResource.getsBranchName());
        resource.setValue(RANGER_RESOURCE_NONE_SCHEMA_VERSION, "ANY_VALUE");

        return resource;
    }

    private RangerAccessResourceImpl registryResource2RangerResources(SchemaVersionResource schemaVersionResource) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();

        SchemaBranchResource schemaBranchResource = schemaVersionResource.getSchemaBranchResource();
        SchemaMetadataResource schemaMetadataResource = schemaBranchResource.getSchemaMetadataResource();

        resource.setValue(RANGER_RESOURCE_SCHEMA_GROUP, schemaMetadataResource.getsGroupName());
        resource.setValue(schemaMetadataResource.getResourceName(), schemaMetadataResource.getsMetadataName());
        resource.setValue(schemaBranchResource.getResourceName(), schemaBranchResource.getsBranchName());
        resource.setValue(schemaVersionResource.getResourceName(), "ANY_VALUE");

        return resource;
    }

}
