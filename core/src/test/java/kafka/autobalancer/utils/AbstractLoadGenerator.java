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

package kafka.autobalancer.utils;

import org.apache.kafka.common.utils.KafkaThread;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractLoadGenerator implements Runnable {
    protected final String topic;
    protected final int partition;
    protected static final int MSG_SIZE = 4096; // 4K
    protected final AtomicInteger msgFailed;
    protected final AtomicInteger msgSucceed;
    protected boolean shutdown;
    private final KafkaThread thread;

    public AbstractLoadGenerator(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
        this.thread = KafkaThread.daemon("load-generator", this);
        this.msgFailed = new AtomicInteger(0);
        this.msgSucceed = new AtomicInteger(0);
    }

    public void start() {
        this.shutdown = false;
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown = true;
        try {
            this.thread.join();
        } catch (InterruptedException ignored) {

        }
    }

    public int msgFailed() {
        return msgFailed.get();
    }

    public int msgSucceed() {
        return msgSucceed.get();
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }
}
