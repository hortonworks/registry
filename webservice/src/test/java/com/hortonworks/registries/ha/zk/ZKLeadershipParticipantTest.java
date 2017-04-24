/*
 * Copyright 2016 Hortonworks.
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

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ZKLeadershipParticipantTest {
    private static final Logger LOG = LoggerFactory.getLogger(ZKLeadershipParticipantTest.class);
    private TestingServer testingServer;

    @Before
    public void startZooKeeper() throws Exception {
        testingServer = new TestingServer(true);
    }

    @After
    public void stopZooKeeper() throws IOException {
        testingServer.stop();
    }

    @Test
    public void testClients() throws Exception {
        Map<String, Object> conf = createConf();

        String participant1 = "foo-1";
        ZKLeadershipParticipant zkLeadershipParticipant = new ZKLeadershipParticipant();
        zkLeadershipParticipant.init(conf, participant1);

        String participant2 = "foo-2";
        ZKLeadershipParticipant zkLeadershipParticipant2 = new ZKLeadershipParticipant();
        zkLeadershipParticipant2.init(conf, participant2);

        zkLeadershipParticipant.participateForLeadership();
        zkLeadershipParticipant2.participateForLeadership();

        waitForLeaderLatchNodeCreation(zkLeadershipParticipant, 10_000);

        String currentLeader = zkLeadershipParticipant.getCurrentLeader();
        LOG.info("########### currentLeader [{}] " + currentLeader);
        zkLeadershipParticipant.exitFromLeaderParticipation();
        LOG.info("Exiting from leader participation: [{}]", participant1);

        currentLeader = zkLeadershipParticipant2.getCurrentLeader();
        LOG.info("########### currentLeader [{}] " + currentLeader);
        Assert.assertEquals(participant2, currentLeader);
    }

    private void waitForLeaderLatchNodeCreation(ZKLeadershipParticipant zkLeadershipParticipant, long waitTimeMillis) throws Exception {
        long startTime = System.currentTimeMillis();

        while(System.currentTimeMillis() - startTime < waitTimeMillis) {
            if(zkLeadershipParticipant.checkLeaderLatchPathExists()) {
                return;
            }
            Thread.sleep(100);
        }

        throw new Exception("Leader latch path not yet created with in "+waitTimeMillis+" millis");
    }

    private Map<String, Object> createConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("connect.url", testingServer.getConnectString());
        conf.put("root", "/root");
        conf.put("session.timeout.ms", 30000);
        conf.put("connection.timeout.ms", 20000);
        conf.put("retry.limit", 5);
        conf.put("retry.base.sleep.time.ms", 1000);
        conf.put("retry.max.sleep.time.ms", 5000);
        return conf;
    }
}
