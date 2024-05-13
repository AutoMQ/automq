/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import com.automq.stream.utils.Threads;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionStatusTracker {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionStatusTracker.class);
    private static final long UNEXPECTED_STATUS_TIMEOUT_MS = 60000;
    private final Map<TopicPartition, Tracker> trackers = new ConcurrentHashMap<>();
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
    }

    public Tracker tracker(TopicPartition tp) {
        Tracker tracker = trackers.computeIfAbsent(tp, Tracker::new);
        tracker.retain();
        return tracker;
    }

    void check() {
        long now = time.milliseconds();
        trackers.forEach((tp, tracker) -> {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (tracker) {
                if (now - tracker.lastUpdateMs <= UNEXPECTED_STATUS_TIMEOUT_MS) {
                    return;
                }
                if (tracker.expectedStatus != tracker.currentStatus || tracker.expectedLeaderEpoch != tracker.currentLeaderEpoch) {
                    LOGGER.error("The partition={} status is unexpected. Expected ({}, {}), but current is ({}, {})",
                        tp, tracker.expectedStatus, tracker.currentStatus, tracker.expectedLeaderEpoch, tracker.currentLeaderEpoch);

                    if (tracker.expectedStatus == Status.LEADER && tracker.currentStatus == Status.OPENED) {
                        tryElectLeaderFunc.accept(tp);
                    }
                }
            }
        });
    }

    public class Tracker extends AbstractReferenceCounted {
        private final TopicPartition tp;
        private Status expectedStatus;
        private int expectedLeaderEpoch;
        private Status currentStatus;
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
            release();
        }

        public synchronized void failed() {
            currentStatus = Status.FAILED;
            lastUpdateMs = time.milliseconds();
        }

        @Override
        protected void deallocate() {
            trackers.remove(tp, this);
        }

        @Override
        public ReferenceCounted touch(Object o) {
            return this;
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
}
