/**
 * Copyright 2016 Hortonworks.
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
 **/
package com.hortonworks.registries.schemaregistry.webservice;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.hortonworks.registries.common.ha.LeadershipParticipant;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseRegistryResource {

    private static final Logger LOG = LoggerFactory.getLogger(BaseRegistryResource.class);

    final AtomicReference<LeadershipParticipant> leadershipParticipant;
    final ISchemaRegistry schemaRegistry;

    // Hack: Adding number in front of sections to get the ordering in generated swagger documentation correct
    static final String OPERATION_GROUP_SCHEMA = "1. Schema";
    static final String OPERATION_GROUP_SERDE = "2. Serializer/Deserializer";
    static final String OPERATION_GROUP_OTHER = "3. Other";



    BaseRegistryResource(ISchemaRegistry schemaRegistry, AtomicReference<LeadershipParticipant> leadershipParticipant) {
        Preconditions.checkNotNull(schemaRegistry, "SchemaRegistry can not be null");
        Preconditions.checkNotNull(leadershipParticipant, "LeadershipParticipant can not be null");

        this.schemaRegistry = schemaRegistry;
        this.leadershipParticipant = leadershipParticipant;
    }

    /**
     * Checks whether the current instance is a leader. If so, it invokes the given {@code supplier}, else current
     * request is redirected to the leader node in registry cluster.
     *
     * @param uriInfo
     * @param supplier
     * @return
     */
    Response handleLeaderAction(UriInfo uriInfo, Supplier<Response> supplier) {
        LOG.info("URI info [{}]", uriInfo.getRequestUri());
        if (!leadershipParticipant.get().isLeader()) {
            URI location = null;
            try {
                String currentLeaderLoc = leadershipParticipant.get().getCurrentLeader();
                URI leaderServerUrl = new URI(currentLeaderLoc);
                URI requestUri = uriInfo.getRequestUri();
                location = new URI(leaderServerUrl.getScheme(), leaderServerUrl.getAuthority(),
                                   requestUri.getPath(), requestUri.getQuery(), requestUri.getFragment());
                LOG.info("Redirecting to URI [{}] as this instance is not the leader", location);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Response.temporaryRedirect(location).build();
        } else {
            LOG.info("Invoking here as this instance is the leader");
            return supplier.get();
        }
    }

    static void checkValueAsNullOrEmpty(String name, String value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Parameter " + name + " is null");
        }
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Parameter " + name + " is empty");
        }
    }
    
}
