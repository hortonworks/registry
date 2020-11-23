/*
 * Copyright 2016-2020 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.authorizer.agent.util;

import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestAuthorizer implements Authorizer {

    private List<Policy> policies = new ArrayList<>();

    public void configure(Map<String, Object> props) { }

    @Override
    public boolean authorize(Resource resource, AccessType accessType, UserAndGroups userAndGroups) {
        for(Policy p : policies) {
            if(resourcesEqual(resource, p.resource)
               && p.accessTypes.contains(accessType)
            && (p.users.contains(userAndGroups.getUser()))) {
                return true;
            }
        }

        return false;
    }

    public void addPolicy(Policy p) {
        policies.add(p);
    }

    public static class Policy {
        private Resource resource;
        private List<String> users;
        private Set<AccessType> accessTypes;


        public Policy(Resource resource, String user, AccessType... accessTypes) {
            this(resource, new String[]{user}, accessTypes);
        }

        public Policy(Resource resource, String[] users, AccessType... accessTypes) {
            this.resource = resource;
            this.users = Arrays.asList(users);
            if(accessTypes == null || accessTypes.length == 0) {
                throw new IllegalArgumentException("accessTypes cannot be empty");
            }
            this.accessTypes = new HashSet<>(Arrays. asList(accessTypes));
        }
    }

    private static boolean resourcesEqual(Resource a, Resource b) {
        if (a == null || b == null || a.getResourceType() != b.getResourceType()) {
            return false;
        }

        if(a instanceof SchemaMetadataResource) {
            SchemaMetadataResource smA = (SchemaMetadataResource) a;
            SchemaMetadataResource smB = (SchemaMetadataResource) b;
            if(!smA.getsGroupName().equals(smB.getsGroupName())
                    || !smA.getsMetadataName().equals(smB.getsMetadataName())) {
                return false;
            }
        }

        if(a instanceof SchemaBranchResource ||
           a instanceof SchemaVersionResource) {
            SchemaBranchResource sbA = (SchemaBranchResource) a;
            SchemaBranchResource sbB = (SchemaBranchResource) b;
            if(!sbA.getsBranchName().equals(sbB.getsBranchName())) {
                return false;
            }
        }

        switch (a.getResourceType()) {
            case SERDE: case SCHEMA_METADATA: case SCHEMA_BRANCH: case SCHEMA_VERSION: return true;
            default: throw new RuntimeException("Resource " + a.getResourceType().name() + " is not supported");
        }

    }

}
