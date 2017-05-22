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

/**
 * Created by pearcem on 22/05/2017.
 */
abstract class BaseRegistryResource {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryResource.class);

    final AtomicReference<LeadershipParticipant> leadershipParticipant;
    final ISchemaRegistry schemaRegistry;

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
