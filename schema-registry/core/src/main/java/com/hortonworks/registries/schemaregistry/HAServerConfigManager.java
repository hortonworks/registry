package com.hortonworks.registries.schemaregistry;

import com.google.common.collect.Sets;
import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Set;

public class HAServerConfigManager {

    Set<String> hostIps = Sets.newConcurrentHashSet();
    private String serverUrl;
    private static final Logger LOG = LoggerFactory.getLogger(HAServerConfigManager.class);
    public static Integer MAX_RETRY = 5;

    public void refresh(Collection<HostConfigStorable> hostConfigStorableList) {
        if (hostConfigStorableList != null) {
            hostConfigStorableList.stream().filter(hostConfigStorable -> !hostConfigStorable.getHostUrl().equals(serverUrl)).forEach(hostConfig -> {
                hostIps.add(hostConfig.getHostUrl());
            });
        }
    }

    public void notifyDebut() {
        notify("api/v1/schemaregistry/register/node/debut", serverUrl);
    }

    public void notifyCacheInvalidation(SchemaRegistryCacheType schemaRegistryCacheType, String keyAsString) {
        notify(String.format("api/v1/schemaregistry/cache/invalidate/%s",schemaRegistryCacheType.name()), keyAsString);
    }

    private void notify(String urlPath, Object postBody) {
        // If Schema Registry was not started in HA mode then serverURL would be null, in case don't bother making POST calls
        if(serverUrl != null) {
            PriorityQueue<Pair<Integer, String>> queue = new PriorityQueue<>();
            hostIps.stream().forEach(hostIp -> {
                queue.add(Pair.of(0, hostIp));
            });

            while (!queue.isEmpty()) {
                Pair<Integer, String> priorityWithHostIp = queue.remove();

                WebTarget target = ClientBuilder.newClient().target(String.format("%s%s", priorityWithHostIp.getRight(), urlPath));
                Response response = null;

                try {
                   response = target.request().post(Entity.json(postBody));
                } catch (Exception e) {
                    LOG.debug("Failed to notify server '{}' about the current host debut in HA mode", priorityWithHostIp.getRight());
                }

                if ( (response == null || response.getStatus() != Response.Status.OK.getStatusCode()) && priorityWithHostIp.getLeft() < MAX_RETRY) {
                    queue.add(Pair.of(priorityWithHostIp.getLeft() + 1, priorityWithHostIp.getRight()));
                } else if (priorityWithHostIp.getLeft() < MAX_RETRY ) {
                    LOG.debug("Notified server '{}' about the current host debut in HA mode", priorityWithHostIp.getRight());
                } else if (priorityWithHostIp.getLeft() >= MAX_RETRY) {
                    LOG.debug("Failed to notify server '{}' about the current host debut in HA mode, giving up as reached max of {} attempts",
                            priorityWithHostIp.getRight(), MAX_RETRY);
                }
            }
        }
    }

    public void addNodeUrl(String nodeUrl) {
        hostIps.add(nodeUrl);
    }

    public void setHomeNodeURL(String homeNodeURL) {
        this.serverUrl = homeNodeURL;
    }
}
