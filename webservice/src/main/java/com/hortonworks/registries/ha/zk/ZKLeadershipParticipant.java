/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.ha.zk;

import com.google.common.base.Preconditions;
import com.hortonworks.registries.common.ha.LeadershipParticipant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link LeadershipParticipant} implementation for ZooKeeper.
 */
public class ZKLeadershipParticipant implements LeadershipParticipant {

    public static final String LEADER_LOCK_NODE_PATH = "-leader-lock";
    public static final int DEFAULT_CONN_TIMOUT = 20_1000;
    public static final int DEFAULT_SESSION_TIMEOUT = 30_1000;
    public static final int DEFAULT_BASE_SLEEP_TIME = 1000;
    public static final int DEFAULT_MAX_SLEEP_TIME = 5000;
    public static final int DEFAULT_RETRY_LIMIT = 5;

    private static final Logger LOG = LoggerFactory.getLogger(ZKLeadershipParticipant.class);
    public static final String CONNECT_URL = "connect.url";
    public static final String CONNECTION_TIMEOUT_MS = "connection.timeout.ms";
    public static final String SESSION_TIMEOUT_MS = "session.timeout.ms";
    public static final String RETRY_BASE_SLEEP_TIME_MS = "retry.base.sleep.time.ms";
    public static final String RETRY_MAX_SLEEP_TIME_MS = "retry.max.sleep.time.ms";
    public static final String RETRY_LIMIT = "retry.limit";

    private CuratorFramework curatorFramework;
    private Map<String, Object> conf;
    private String serverUrl;
    private LeaderLatchListener leaderLatchListener;
    private AtomicReference<LeaderLatch> leaderLatchRef;
    private String leaderLatchPath;

    public void init(Map<String, Object> conf, String participantId) {
        Preconditions.checkNotNull(participantId, "participantId can not be null");
        Preconditions.checkNotNull(conf, "conf can not be null");

        this.conf = conf;
        this.serverUrl = participantId;
        this.leaderLatchListener = createLeaderLatchListener();

        LOG.info("Received configuration : [{}]", conf);

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        String url = (String) conf.get(CONNECT_URL);
        String rootPrefix = (String) conf.get("root");
        builder.connectString(url);
        builder.connectionTimeoutMs((Integer) conf.getOrDefault(CONNECTION_TIMEOUT_MS, DEFAULT_CONN_TIMOUT));
        builder.sessionTimeoutMs((Integer) conf.getOrDefault(SESSION_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT));

        builder.retryPolicy(
                new BoundedExponentialBackoffRetry(
                        (Integer) conf.getOrDefault(RETRY_BASE_SLEEP_TIME_MS, DEFAULT_BASE_SLEEP_TIME),
                        (Integer) conf.getOrDefault(RETRY_MAX_SLEEP_TIME_MS, DEFAULT_MAX_SLEEP_TIME),
                        (Integer) conf.getOrDefault(RETRY_LIMIT, DEFAULT_RETRY_LIMIT)

                ));

        curatorFramework = builder.build();
        leaderLatchPath = rootPrefix + LEADER_LOCK_NODE_PATH;
        leaderLatchRef = new AtomicReference<>(createLeaderLatch());
        curatorFramework.start();
    }

    private LeaderLatchListener createLeaderLatchListener() {
        return new LeaderLatchListener() {
            @Override
            public void isLeader() {
                LOG.info("This instance with id [{}] acquired leadership", serverUrl);
            }

            @Override
            public void notLeader() {
                LOG.info("This instance with id [{}] lost leadership", serverUrl);
            }
        };
    }

    private LeaderLatch createLeaderLatch() {
        return new LeaderLatch(curatorFramework, leaderLatchPath, serverUrl);
    }

    public boolean checkLeaderLatchPathExists() {
        try {
            curatorFramework.getChildren().forPath(leaderLatchPath);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Participates for leader lock with the given configuration.
     *
     * @throws Exception if any errors encountered.
     */
    @Override
    public void participateForLeadership() throws Exception {
        // if the existing leader latch is closed, recreate and connect again
        if (LeaderLatch.State.CLOSED.equals(leaderLatchRef.get().getState())) {
            // remove listener from earlier closed leader latch
            leaderLatchRef.get().removeListener(leaderLatchListener);

            leaderLatchRef.set(createLeaderLatch());
            leaderLatchRef.get().addListener(leaderLatchListener);
            LOG.info("Existing leader latch is in CLOSED state, it is recreated.");
        }

        // if the existing leader latch is not yet started, start now!!
        if (LeaderLatch.State.LATENT.equals(leaderLatchRef.get().getState())) {
            leaderLatchRef.get().start();
            LOG.info("Existing leader latch is in LATENT state, it is started. leader latch: [{}]", leaderLatchRef.get());
        }
    }

    /**
     * Returns the current leader's participant id.
     *
     * @throws Exception if any error occurs.
     */
    @Override
    public String getCurrentLeader() throws Exception {
        return leaderLatchRef.get().getLeader().getId();
    }

    /**
     * Exits the current leader latch by closing it. This may throw an Exception if the current latch is not yet started
     * or it has already been closed.
     *
     * @throws IOException if any IO related errors occur.
     */
    @Override
    public void exitFromLeaderParticipation() throws IOException {
        // close the current leader latch for removing from leader participation.
        leaderLatchRef.get().close();
    }

    /**
     * Returns true if the current participant is a leader
     */
    @Override
    public boolean isLeader() {
        return leaderLatchRef.get().hasLeadership();
    }

    /**
     * Closes the underlying ZK client resources.
     *
     * @throws IOException if any io errors occurred during this operation.
     */
    @Override
    public void close() throws IOException {
        leaderLatchRef.get().close();
        curatorFramework.close();
    }

    @Override
    public String toString() {
        return "ZKLeadershipParticipant{" +
                "conf=" + conf +
                ", serverUrl='" + serverUrl + '\'' +
                ", leaderLatchPath='" + leaderLatchPath + '\'' +
                '}'+super.toString();
    }
}
