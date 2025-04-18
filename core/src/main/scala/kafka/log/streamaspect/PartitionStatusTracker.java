/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.log.streamaspect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public class PartitionStatusTracker {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionStatusTracker.class);
    private static final long UNEXPECTED_STATUS_TIMEOUT_MS = 60000;
    private static final String UNEXPECTED_STATUS = "UNEXPECTED";
    final Map<TopicPartition, Tracker> trackers = new ConcurrentHashMap<>();
    private Statistics statistics = new Statistics();
    private final ReentrantLock lock = new ReentrantLock();

    private final Time time;
    private final Consumer<TopicPartition> tryElectLeaderFunc;

    public PartitionStatusTracker(Time time, Consumer<TopicPartition> tryElectLeaderFunc) {
        this.time = time;
        this.tryElectLeaderFunc = tryElectLeaderFunc;
        Threads.COMMON_SCHEDULER.scheduleWithFixedDelay(() -> {
            try {
                check();
            } catch (Throwable t) {
                LOGGER.error("Error in partition status tracker check", t);
            }
        }, 30, 30, TimeUnit.SECONDS);
        S3StreamKafkaMetricsManager.setPartitionStatusStatisticsSupplier(Statistics.statusNames(), n -> PartitionStatusTracker.this.statistics.get(n));
    }

    public Tracker tracker(TopicPartition tp) {
        lock.lock();
        try {
            Tracker tracker = trackers.computeIfAbsent(tp, Tracker::new);
            tracker.retain();
            return tracker;
        } finally {
            lock.unlock();
        }
    }

    void remove(TopicPartition tp, Tracker tracker) {
        lock.lock();
        try {
            trackers.remove(tp, tracker);
        } finally {
            lock.unlock();
        }
    }

    void check() {
        long now = time.milliseconds();
        Statistics statistics = new Statistics();
        trackers.forEach((tp, tracker) -> {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (tracker) {
                statistics.statusCount.merge(tracker.currentStatus.name(), 1, Integer::sum);

                if (now - tracker.lastUpdateMs <= UNEXPECTED_STATUS_TIMEOUT_MS) {
                    return;
                }
                if (tracker.expectedStatus != tracker.currentStatus || tracker.expectedLeaderEpoch != tracker.currentLeaderEpoch) {
                    statistics.statusCount.merge(UNEXPECTED_STATUS, 1, Integer::sum);
                    LOGGER.error("The partition={} status is unexpected. Expected ({}, {}), but current is ({}, {})",
                        tp, tracker.expectedStatus, tracker.expectedLeaderEpoch, tracker.currentStatus, tracker.currentLeaderEpoch);

                    if (tracker.expectedStatus == Status.LEADER && tracker.currentStatus == Status.OPENED) {
                        tryElectLeaderFunc.accept(tp);
                    }
                }
            }
        });
        this.statistics = statistics;
    }

    public class Tracker extends AbstractReferenceCounted {
        private final TopicPartition tp;
        private Status expectedStatus;
        private int expectedLeaderEpoch;
        private Status currentStatus = Status.UNKNOWN;
        private int currentLeaderEpoch;
        private long lastUpdateMs;

        Tracker(TopicPartition tp) {
            this.tp = tp;
        }

        public synchronized void expected(Status status, int leaderEpoch) {
            expectedStatus = status;
            expectedLeaderEpoch = leaderEpoch;
            lastUpdateMs = time.milliseconds();
        }

        public synchronized void expected(Status status) {
            expected(status, expectedLeaderEpoch);
        }

        public synchronized void opening(int leaderEpoch) {
            transition(Status.OPENING);
            currentLeaderEpoch = leaderEpoch;
        }

        public synchronized void opened() {
            transition(Status.OPENED);
        }

        public synchronized void leader() {
            transition(Status.LEADER);
        }

        public synchronized void closing() {
            transition(Status.CLOSING);
        }

        public synchronized void closed() {
            transition(Status.CLOSED);
            lock.lock();
            try {
                if (refCnt() == 1) {
                    // cleanup the tracker if no one is using it
                    release();
                }
            } finally {
                lock.unlock();
            }
        }

        public synchronized void failed() {
            currentStatus = Status.FAILED;
            lastUpdateMs = time.milliseconds();
        }

        @Override
        protected void deallocate() {
            remove(tp, this);
        }

        @Override
        public ReferenceCounted touch(Object o) {
            return this;
        }

        @Override
        public ReferenceCounted retain() {
            lock.lock();
            try {
                return super.retain();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean release() {
            lock.lock();
            try {
                return super.release();
            } finally {
                lock.unlock();
            }
        }

        void transition(Status status) {
            currentStatus = status;
            lastUpdateMs = time.milliseconds();
        }
    }

    public enum Status {
        UNKNOWN,
        OPENING,
        OPENED,
        LEADER,
        CLOSING,
        CLOSED,
        FAILED,
    }

    static class Statistics {
        final Map<String, Integer> statusCount = new HashMap<>();

        public static List<String> statusNames() {
            List<String> rst = Arrays.stream(Status.values()).map(Enum::name).collect(Collectors.toList());
            rst.add(UNEXPECTED_STATUS);
            return rst;
        }

        public Integer get(String status) {
            return statusCount.getOrDefault(status, 0);
        }
    }
}
