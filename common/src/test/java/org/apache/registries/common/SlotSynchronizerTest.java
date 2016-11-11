/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.registries.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class SlotSynchronizerTest {

    @Test
    public void stressTestSlotLocks() throws Exception {
        SlotSynchronizer<Integer> slotSynchronizer = new SlotSynchronizer<>();
        int key = new Random().nextInt();

        int size = 64;
        long limit = 1000_000;
        AtomicLong aggregatedCount = new AtomicLong();
        List<Thread> threads = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            threads.add(new Thread(new Work(key, slotSynchronizer, limit, aggregatedCount)));
        }
        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(limit * size, aggregatedCount.get());
    }

    private static class Work implements Runnable {
        private final int k;
        private final SlotSynchronizer<Integer> slotSynchronizer;
        private final long limit;
        private final AtomicLong count;

        private Work(int k, SlotSynchronizer<Integer> slotSynchronizer, long limit, AtomicLong count) {
            this.k = k;
            this.slotSynchronizer = slotSynchronizer;
            this.limit = limit;
            this.count = count;
        }

        @Override
        public void run() {
            for (int i = 0; i < limit; i++) {
                SlotSynchronizer.Lock lock = slotSynchronizer.lockSlot(k);
                count.incrementAndGet();
                slotSynchronizer.unlock(lock);
            }
        }
    }

}
