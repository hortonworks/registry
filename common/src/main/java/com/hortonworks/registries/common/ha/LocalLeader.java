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
package com.hortonworks.registries.common.ha;

import java.io.IOException;
import java.util.Map;

/**
 * This is an implementation for {@link LeadershipParticipant} which considers itself as the leader. This is used when HA is
 * not enabled on registry instances to consider the registry instance as leader.
 */
public final class LocalLeader implements LeadershipParticipant {
    private static final LocalLeader instance = new LocalLeader();

    public static LocalLeader getInstance() {
        return instance;
    }

    private LocalLeader() {
    }

    @Override
    public void init(Map<String, Object> config, String participantId) {
    }

    @Override
    public void participateForLeadership() throws Exception {
    }

    @Override
    public String getCurrentLeader() throws Exception {
        return "MYSELF";
    }

    @Override
    public void exitFromLeaderParticipation() throws IOException {
    }

    /**
     * Always returns true as it considers the current instance is always the leader.
     */
    @Override
    public boolean isLeader() {
        return true;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public String toString() {
        return "LocalLeader{}";
    }
}
