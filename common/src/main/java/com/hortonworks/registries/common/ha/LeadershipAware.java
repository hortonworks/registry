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

import java.util.concurrent.atomic.AtomicReference;

/**
 * This should be implemented by modules which need awareness of {@link LeadershipParticipant} registration and they can
 * participate in leader election.
 */
public interface LeadershipAware {

    /**
     * Sets the configured leadership client instance.
     */
    public void setLeadershipParticipant(AtomicReference<LeadershipParticipant> leadershipParticipant);
}
