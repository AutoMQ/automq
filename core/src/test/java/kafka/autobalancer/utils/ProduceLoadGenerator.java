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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.test.TestUtils;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProduceLoadGenerator extends AbstractLoadGenerator {
    private final Properties props;
    private final byte[] payload;
    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();
    private final ScheduledExecutorService executorService;
    private final int throughput;
    private int token;

    public ProduceLoadGenerator(Properties producerProps, String topic, int partition, int load) {
        super(topic, partition);
        this.props = producerProps;
        this.throughput = load * 1024 / MSG_SIZE;
        this.payload = new byte[MSG_SIZE];
        generatePayload(payload);
        this.token = throughput;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    private void generatePayload(byte[] payload) {
        for (int j = 0; j < payload.length; ++j)
            payload[j] = (byte) (TestUtils.RANDOM.nextInt(26) + 65);
    }

    @Override
    public void start() {
        super.start();
        this.executorService.scheduleAtFixedRate(() -> {
            lock.lock();
            try {
                this.token = throughput;
                cond.signal();
            } finally {
                lock.unlock();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        try (final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(this.props)) {
            while (!shutdown) {
                lock.lock();
                try {
                    while (this.token == 0) {
                        cond.await();
                    }
                    this.token--;
                } catch (InterruptedException ignored) {

                } finally {
                    lock.unlock();
                }
                try {
                    producer.send(new ProducerRecord<>(topic, partition, null, payload), (m, e) -> {
                        if (e != null) {
                            msgFailed.incrementAndGet();
                        }
                    });
                    msgSucceed.incrementAndGet();
                } catch (Exception e) {
                    msgFailed.incrementAndGet();
                }
            }
        }
    }
}
