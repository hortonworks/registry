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
 * Participant to connect to target systems(like ZooKeeper) which give a way to elect a leader among the participants.
 */
public interface LeadershipParticipant {

    /**
     * Initializes with the given {@code config} and {@code participantId}.
     *
     * @param config configuration required for participating in leader election
     * @param participantId Id of this participant which should be unique identifier among all the participants for
     *                      a given config.
     */
    void init(Map<String, Object> config, String participantId);

    /**
     * Participates for leadership with the given configuration.
     *
     * @throws Exception if any errors encountered.
     */
    void participateForLeadership() throws Exception;

    /**
     * Returns the current leader's participant id.
     * @throws Exception if any error occurs.
     */
    String getCurrentLeader() throws Exception;

    /**
     * Exits from leadership participation. This may throw an Exception if the current latch is not yet started
     * or it has already been closed.
     *
     * @throws IOException if any IO related errors occur.
     */
    void exitFromLeaderParticipation() throws IOException;

    /**
     * Returns true if the current participant is a leader
     */
    boolean isLeader();

    /**
     * Closes the underlying ZK client resources.
     *
     * @throws IOException if any io errors occurred during this operation.
     */
    void close() throws IOException;

}
