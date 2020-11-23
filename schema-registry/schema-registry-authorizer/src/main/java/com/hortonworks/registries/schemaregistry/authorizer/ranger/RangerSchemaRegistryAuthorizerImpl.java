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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;

import java.util.Map;

public class RangerSchemaRegistryAuthorizerImpl implements Authorizer {
    
    private static final Logger LOG = LoggerFactory.getLogger(RangerSchemaRegistryAuthorizerImpl.class);

    ///////////// List of Ranger specific resources //////////////////////////

    private static final String RANGER_RESOURCE_REGISTRY_SERVICE = "registry-service";
    private static final String RANGER_RESOURCE_SERDE = "serde";
    private static final String RANGER_RESOURCE_SCHEMA_GROUP = "schema-group";
    private static final String RANGER_RESOURCE_SCHEMA_METADATA = "schema-metadata";
    private static final String RANGER_RESOURCE_SCHEMA_BRANCH = "schema-branch";
    private static final String RANGER_RESOURCE_SCHEMA_VERSION = "schema-version";

    private final RangerBasePlugin plg;

    public RangerSchemaRegistryAuthorizerImpl() {
        this.plg = SchemaRegistryRangerPlugin.getInstance();
    }

    @Override
    public void configure(Map<String, Object> props) { }

    @Override
    public boolean authorize(Resource registryResource,
                             AccessType accessType,
                             UserAndGroups userAndGroups) {

        return authorize(registryResource2RangerResource(registryResource), accessType, userAndGroups)
                || authorizeRangerSchemaRegistryResource(accessType, userAndGroups);
    }

    private boolean authorize(RangerAccessResourceImpl resource,
                              AccessType accessType,
                              UserAndGroups userAndGroups) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, accessType.getName(),
                userAndGroups.getUser(),
                userAndGroups.getGroups());

        RangerAccessResult res = plg.isAccessAllowed(request);
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Ranger request: %s", request));
            LOG.trace(String.format("Ranger request result: %s", res));
        }

        return res != null && res.getIsAllowed();
    }

    RangerAccessResourceImpl registryResource2RangerResource(Resource registryResource) {
        RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();

        if(registryResource instanceof SchemaMetadataResource) {
            SchemaMetadataResource smr = (SchemaMetadataResource) registryResource;
            rangerResource.setValue(RANGER_RESOURCE_SCHEMA_GROUP, smr.getsGroupName());
            rangerResource.setValue(RANGER_RESOURCE_SCHEMA_METADATA, smr.getsMetadataName());
        }

        if(registryResource instanceof SchemaBranchResource) {
            SchemaBranchResource sbr = (SchemaBranchResource) registryResource;
            rangerResource.setValue(RANGER_RESOURCE_SCHEMA_BRANCH, sbr.getsBranchName());
        }

        switch (registryResource.getResourceType()) {
            case SERDE: {
                rangerResource.setValue(RANGER_RESOURCE_SERDE, "ANY_VALUE");
                return rangerResource;
            }
            case SCHEMA_VERSION: {
                rangerResource.setValue(RANGER_RESOURCE_SCHEMA_VERSION, "ANY_VALUE");
                return rangerResource;
            }
            case SCHEMA_METADATA: case SCHEMA_BRANCH: {
                return rangerResource;
            }

            default:
                // In current implemetataion the exception should never be thrown. This is added for future if
                // the set of resources is extended but implemetation is not provided.
                throw new RuntimeException(
                        String.format("Cannot convert registry resource to ranger resource. ResourceType %s is not supported",
                                registryResource.getResourceType().name()));
        }

    }

    boolean authorizeRangerSchemaRegistryResource(AccessType accessType, UserAndGroups userAndGroups) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue(RANGER_RESOURCE_REGISTRY_SERVICE, "ANY_VALUE");

        return authorize(resource, accessType, userAndGroups);
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

        public static SchemaRegistryRangerPlugin getInstance() {
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
