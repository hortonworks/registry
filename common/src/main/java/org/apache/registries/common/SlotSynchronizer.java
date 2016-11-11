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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class SlotSynchronizer<K> {

    private final ConcurrentHashMap<K, Lock> locks = new ConcurrentHashMap<>();

    public static final class Lock<K> {
        private final K k;
        private AtomicInteger count = new AtomicInteger();
        private ReentrantLock reentrantLock = new ReentrantLock();

        public Lock(K k) {
            this.k = k;
        }

        private void increment() {
            count.incrementAndGet();
        }

        private void decrement() {
            count.decrementAndGet();
        }

        private void lock() {
            reentrantLock.lock();
        }

        private void unlock() {
            reentrantLock.unlock();
        }
    }

    /**
     * Returns the lock for the given slot {@code k}
     * @param k
     * @return
     */
    public Lock lockSlot(K k) {
        while (true) {
            Lock<K> newLock = new Lock<>(k);
            Lock lock = locks.putIfAbsent(k, newLock);
            if (lock == null) {
                lock = newLock;
            }

            // increment and wait to acquire lock
            lock.increment();
            lock.lock();

            // below is possible when current lock is incremented after removing that from slot by earlier unlock
            // if acquired lock is not same as the current lock, retry again.
            if (locks.get(k) != lock) {
                continue;
            }

            return lock;
        }
    }

    /**
     * Unlocks the given slot and blocks till it is unlocked.
     * @param lock
     */
    public void unlock(Lock lock) {
        // decrement the count and remove this slot if none is registered now.
        lock.decrement();
        if (lock.count.get() == 0) {
            locks.remove(lock.k, lock);
        }

        // may be possible some one registered to take lock after removing the slot and calling unlock below.
        lock.unlock();
    }

}
