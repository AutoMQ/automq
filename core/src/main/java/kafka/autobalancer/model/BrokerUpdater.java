/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.autobalancer.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BrokerUpdater {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerUpdater.class);
    private final Lock lock = new ReentrantLock();
    private final Broker broker;

    public BrokerUpdater(int brokerId, boolean active) {
        this.broker = new Broker(brokerId, active);
    }

    public Broker get() {
        Broker broker;
        lock.lock();
        try {
            // Broker is fenced or in shutdown.
            // For fenced or shutting-down brokers, the replicationControlManager will handle the movement of partitions.
            // So we do not need to consider them in auto-balancer.
            if (!this.broker.isActive()) {
                return null;
            }
            broker = new Broker(this.broker);
        } finally {
            lock.unlock();
        }
        return broker;
    }

    public void setActive(boolean active) {
        lock.lock();
        try {
            this.broker.setActive(active);
        } finally {
            lock.unlock();
        }
    }


}
