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

import com.hortonworks.registries.common.HAConfiguration;
import com.hortonworks.registries.common.SecureClient;
import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import com.hortonworks.registries.schemaregistry.retry.RetryExecutor;
import com.hortonworks.registries.schemaregistry.retry.policy.BackoffPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.ExponentialBackoffPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HAServerNotificationManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HAServerNotificationManager.class);

    private static final String RETRY_POLICY_CONFIG_KEY = "config";
    private static final String RETRY_POLICY_CLASS_KEY = "className";

    private static final String DEFAULT_BACKOFF_POLICY = ExponentialBackoffPolicy.class.getCanonicalName();
    private static final int DEFAULT_CACHE_INVALIDATION_THREAD_COUNT = 20;
    private static final long DEFAULT_SCHEDULER_SHUTDOWN_TIMEOUT_MS = 60000;

    private String serverUrl;
    private Set<String> peerServerURLs = new HashSet<>();
    private final ExecutorService executorService;
    private final RetryExecutor retryExecutor;
    private SecureClient client;

    public HAServerNotificationManager(HAConfiguration haConfiguration) {
        client = createClient(haConfiguration);
        retryExecutor = createRetryExecutor(haConfiguration);
        executorService = createExecutorService(haConfiguration);
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
        if (!peerServerURLs.isEmpty()) {
            Set<String> peerServerURLCopy;
            synchronized (HAServerNotificationManager.class) {
                peerServerURLCopy = new HashSet<>(peerServerURLs);
            }

            peerServerURLCopy.forEach(peerURL -> {
                executorService.submit(() -> {
                    try {
                        Response response = retryExecutor.execute(() -> {
                            WebTarget target = client.target(String.format("%s%s", peerURL, urlPath));

                            LOG.debug("Invoking '{}' for cache invalidation", target.getUri().toString());
                            try {
                                return UserGroupInformation.getLoginUser().doAs((PrivilegedAction<Response>) () -> {
                                    return target.request().post(Entity.json(postBody));
                                });
                            } catch (ProcessingException | IOException e) {
                                LOG.warn("Failed to reach the peer server '{}' for cache synchronization. Retrying ...", peerURL);
                                throw new RetryableCacheInvalidationException(e);
                            }
                        });

                        if ((response == null || response.getStatus() != Response.Status.OK.getStatusCode())) {
                            LOG.warn("Invalid response while reaching out to the peer server '{}' for cache synchronization : {}"
                                    , peerURL, response);
                        }

                    } catch (RetryableCacheInvalidationException e) {
                        LOG.warn("Giving up reaching out to the peer server '{}' for cache synchronization\n {}", peerURL, e.getMessage());
                    }
                });
            });
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

    private SecureClient createClient(HAConfiguration haConfiguration) {
        return new SecureClient.Builder().sslConfig(haConfiguration == null ? null : haConfiguration.getSslConfig())
                                         .build();
    }

    private RetryExecutor createRetryExecutor(HAConfiguration haConfiguration) {
        String retryPolicyClass = DEFAULT_BACKOFF_POLICY;
        Map<String, Object> retryPolicyProps = new HashMap<>();

        if (haConfiguration !=null) {
            Map<String, ?> peerCacheInvalidationRetryPolicy = haConfiguration.getCacheInvalidationRetryPolicy();
            if (peerCacheInvalidationRetryPolicy != null) {
                if (peerCacheInvalidationRetryPolicy.containsKey(RETRY_POLICY_CLASS_KEY)) {
                    retryPolicyClass = (String) peerCacheInvalidationRetryPolicy.get(RETRY_POLICY_CLASS_KEY);
                }
                if (peerCacheInvalidationRetryPolicy.containsKey(RETRY_POLICY_CONFIG_KEY)) {
                    retryPolicyProps = (Map<String, Object>) peerCacheInvalidationRetryPolicy.get(RETRY_POLICY_CONFIG_KEY);
                }
            }
        }

        ClassLoader classLoader = this.getClass().getClassLoader();
        BackoffPolicy backoffPolicy;
        Class<? extends BackoffPolicy> clazz = null;

        try {
            clazz = (Class<? extends BackoffPolicy>) Class.forName(retryPolicyClass, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to initiate the retry policy class : " + retryPolicyClass, e);
        }
        try {
            backoffPolicy = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to create an instance of retry policy class : " + retryPolicyClass, e);
        }

        backoffPolicy.init(retryPolicyProps);

        LOG.debug("For HA cache invalidation using the retry policy : {}", backoffPolicy);

        return new RetryExecutor.Builder().backoffPolicy(backoffPolicy)
                                          .retryOnException(RetryableCacheInvalidationException.class)
                                          .build();
    }

    private ExecutorService createExecutorService(HAConfiguration haConfiguration) {
        int executorServiceThreadCount =  DEFAULT_CACHE_INVALIDATION_THREAD_COUNT ;
        if (haConfiguration!=null && haConfiguration.getCacheInvalidationThreadCount() > 0) {
            executorServiceThreadCount = haConfiguration.getCacheInvalidationThreadCount();
        }
        LOG.debug("Building a thread pool of {} threads for HA cache invalidation", executorServiceThreadCount);

        return Executors.newFixedThreadPool(executorServiceThreadCount);
    }

    @Override
    public void close() throws Exception {
        try {
            executorService.shutdown();
            executorService.awaitTermination(DEFAULT_SCHEDULER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } finally {
            if (!executorService.isShutdown()) {
                executorService.shutdownNow();
            }
            LOG.debug("Shutdown thread pool for HA cache invalidation");
        }
    }

    private static class RetryableCacheInvalidationException extends RuntimeException {
        public RetryableCacheInvalidationException(Exception e) {
            super(e);
        }
    }
}
