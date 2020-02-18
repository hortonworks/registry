/**
 * Copyright 2018-2019 Cloudera, Inc.
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

package com.hortonworks.registries.schemaregistry;

import com.hortonworks.registries.common.RegistryHAConfiguration;
import com.hortonworks.registries.common.SecureClient;
import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class HAServerNotificationManager {

    private static final Logger LOG = LoggerFactory.getLogger(HAServerNotificationManager.class);

    private String serverUrl;
    private Set<String> peerServerURLs = new HashSet<>();
    public static Integer MAX_RETRY = 3;
    private SecureClient client;

    public HAServerNotificationManager(RegistryHAConfiguration registryHAConfiguration) {
        client = new SecureClient.Builder()
                                 .sslConfig(registryHAConfiguration == null ? null : registryHAConfiguration.getSslConfig())
                                 .build();
    }

    public void updatePeerServerURLs(Collection<HostConfigStorable> hostConfigStorableList) {
        if (hostConfigStorableList != null) {
            synchronized (HAServerNotificationManager.class) {
                peerServerURLs.clear();
                hostConfigStorableList.stream().filter(hostConfigStorable -> !hostConfigStorable.getHostUrl().equals(serverUrl))
                                               .forEach(hostConfig -> {
                                                   peerServerURLs.add(hostConfig.getHostUrl());
                                                });
            }
        }
    }

    public void notifyDebut() {
        notify("api/v1/schemaregistry/notifications/node/debut", serverUrl);
    }

    public void notifyCacheInvalidation(SchemaRegistryCacheType schemaRegistryCacheType, String keyAsString) {
        notify(String.format("api/v1/schemaregistry/cache/%s/invalidate", schemaRegistryCacheType.name()), keyAsString);
    }

    private void notify(String urlPath, Object postBody) {
        // If Schema Registry was not started in HA mode then serverURL would be null, in case don't bother making POST calls
        if (serverUrl != null) {
            PriorityQueue<Pair<Integer, String>> queue = new PriorityQueue<>();
            synchronized (HAServerNotificationManager.class) {
                peerServerURLs.forEach(serverURL -> {
                    queue.add(Pair.of(1, serverURL));
                });
            }

            while (!queue.isEmpty()) {
                Pair<Integer, String> priorityWithServerURL = queue.remove();

                WebTarget target = client.target(String.format("%s%s", priorityWithServerURL.getRight(), urlPath));
                Response response = null;

                try {
                    response = UserGroupInformation.getLoginUser().doAs((PrivilegedAction<Response>) () -> {
                        return target.request().post(Entity.json(postBody));
                    });
                } catch (Exception e) {
                    LOG.warn("Failed to notify the peer server '{}' about the current host debut.", priorityWithServerURL.getRight());
                }

                if ((response == null || response.getStatus() != Response.Status.OK.getStatusCode()) && priorityWithServerURL.getLeft() < MAX_RETRY) {
                    queue.add(Pair.of(priorityWithServerURL.getLeft() + 1, priorityWithServerURL.getRight()));
                } else if (priorityWithServerURL.getLeft() < MAX_RETRY) {
                    LOG.info("Notified the peer server '{}' about the current host debut.", priorityWithServerURL.getRight());
                } else if (priorityWithServerURL.getLeft() >= MAX_RETRY) {
                    LOG.warn("Failed to notify the peer server '{}' about the current host debut, giving up after {} attempts.",
                            priorityWithServerURL.getRight(), MAX_RETRY);
                }

                try {
                    Thread.sleep(priorityWithServerURL.getLeft() * 100);
                } catch (InterruptedException e) {
                    LOG.warn("Failed to notify the peer server '{}'", priorityWithServerURL.getRight(), e);
                }
            }

        }
    }

    public void addPeerServerURL(String nodeUrl) {
        synchronized (HAServerNotificationManager.class) {
            peerServerURLs.add(nodeUrl);
        }
    }

    public void setServerURL(String homeNodeURL) {
        this.serverUrl = homeNodeURL;
    }

    public String getServerURL() {
        return this.serverUrl;
    }
}
