/**
 * Copyright 2018 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.registries.schemaregistry;

import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class HAServerNotificationManager {

    Set<String> hostIps = new HashSet<>();
    private final String UPDATE_ITERATE_LOCK = "UPDATE_ITERATE_LOCK";
    private String serverUrl;
    private static final Logger LOG = LoggerFactory.getLogger(HAServerNotificationManager.class);
    public static Integer MAX_RETRY = 3;

    public void refreshServerInfo(Collection<HostConfigStorable> hostConfigStorableList) {
        if (hostConfigStorableList != null) {
            synchronized (UPDATE_ITERATE_LOCK) {
                hostIps.clear();
                hostConfigStorableList.stream().filter(hostConfigStorable -> !hostConfigStorable.getHostUrl().equals(serverUrl)).forEach(hostConfig -> {
                    hostIps.add(hostConfig.getHostUrl());
                });
            }
        }
    }

    public void notifyDebut() {
        notify("api/v1/schemaregistry/notifications/node/debut", serverUrl);
    }

    public void notifyCacheInvalidation(SchemaRegistryCacheType schemaRegistryCacheType, String keyAsString) {
        notify(String.format("api/v1/schemaregistry/cache/%s/invalidate",schemaRegistryCacheType.name()), keyAsString);
    }

    private void notify(String urlPath, Object postBody) {
        // If Schema Registry was not started in HA mode then serverURL would be null, in case don't bother making POST calls
        if(serverUrl != null) {
            PriorityQueue < Pair<Integer, String> > queue = new PriorityQueue<>();
            synchronized (UPDATE_ITERATE_LOCK) {
                hostIps.stream().forEach(hostIp -> {
                    queue.add(Pair.of(1, hostIp));
                });
            }

            while (!queue.isEmpty()) {
                Pair<Integer, String> priorityWithHostIp = queue.remove();

                WebTarget target = ClientBuilder.newClient().target(String.format("%s%s", priorityWithHostIp.getRight(), urlPath));
                Response response = null;

                try {
                   response = target.request().post(Entity.json(postBody));
                } catch (Exception e) {
                    LOG.warn("Failed to notify the peer server '{}' about the current host debut.", priorityWithHostIp.getRight());
                }

                if ( (response == null || response.getStatus() != Response.Status.OK.getStatusCode()) && priorityWithHostIp.getLeft() < MAX_RETRY) {
                    queue.add(Pair.of(priorityWithHostIp.getLeft() + 1, priorityWithHostIp.getRight()));
                } else if (priorityWithHostIp.getLeft() < MAX_RETRY ) {
                    LOG.info("Notified the peer server '{}' about the current host debut.", priorityWithHostIp.getRight());
                } else if (priorityWithHostIp.getLeft() >= MAX_RETRY) {
                    LOG.warn("Failed to notify the peer server '{}' about the current host debut, giving up after {} attempts.",
                            priorityWithHostIp.getRight(), MAX_RETRY);
                }

                try {
                    Thread.sleep(priorityWithHostIp.getLeft() * 100);
                } catch (InterruptedException e) {
                    LOG.warn("Failed to notify the peer server '{}'",priorityWithHostIp.getRight(), e);
                }
            }

        }
    }

    public void addNodeUrl(String nodeUrl) {
        synchronized (UPDATE_ITERATE_LOCK) {
            hostIps.add(nodeUrl);
        }
    }

    public void setHomeNodeURL(String homeNodeURL) {
        this.serverUrl = homeNodeURL;
    }

    public String getHomeNodeURL() {
        return this.serverUrl;
    }
}
