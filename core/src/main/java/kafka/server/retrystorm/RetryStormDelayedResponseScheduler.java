/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.server.retrystorm;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

/**
 * Schedules final response send callbacks for retry storm delayed responses.
 *
 * <p>The scheduler does not inspect or mutate Kafka responses. It only owns timer lifecycle,
 * shutdown draining, and exactly-once execution of the supplied final-send callback.</p>
 */
public class RetryStormDelayedResponseScheduler {
    private final long tickMs;
    private final HashedWheelTimer timer;
    private final ConcurrentHashMap<Timeout, ScheduledSend> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();

    /**
     * Creates a scheduler with the default timer tick.
     */
    public RetryStormDelayedResponseScheduler() {
        this(100L);
    }

    /**
     * Creates a scheduler with a caller-defined timer tick in milliseconds.
     */
    public RetryStormDelayedResponseScheduler(long tickMs) {
        this.tickMs = tickMs;
        this.timer = new HashedWheelTimer(tickMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedules the final response send; zero delay or closed scheduler executes the send immediately.
     */
    public void schedule(long delayMs, Runnable sendNow) {
        if (delayMs <= 0) {
            sendNow.run();
            return;
        }

        long roundedDelayMs = roundUpToTick(delayMs);
        ScheduledSend scheduledSend = new ScheduledSend(sendNow);
        try {
            boolean sendImmediately = false;
            synchronized (lifecycleLock) {
                if (closed.get()) {
                    sendImmediately = true;
                } else {
                    Timeout timeout = timer.newTimeout(timeoutRef -> {
                        ScheduledSend scheduled;
                        synchronized (lifecycleLock) {
                            scheduled = pending.remove(timeoutRef);
                        }
                        if (scheduled != null) {
                            scheduled.send();
                        }
                    }, roundedDelayMs, TimeUnit.MILLISECONDS);
                    pending.put(timeout, scheduledSend);
                }
            }
            if (sendImmediately) {
                scheduledSend.send();
            }
        } catch (IllegalStateException e) {
            scheduledSend.send();
        }
    }

    /**
     * Stops accepting new delayed sends and best-effort sends every pending response once.
     */
    public void shutdown() {
        ArrayList<ScheduledSend> sends = new ArrayList<>();
        synchronized (lifecycleLock) {
            closed.set(true);
            for (Map.Entry<Timeout, ScheduledSend> entry : pending.entrySet()) {
                pending.remove(entry.getKey());
                entry.getKey().cancel();
                sends.add(entry.getValue());
            }
        }
        for (ScheduledSend send : sends) {
            send.send();
        }
        timer.stop();
    }

    private long roundUpToTick(long delayMs) {
        if (delayMs <= tickMs) {
            return tickMs;
        }
        return ((delayMs + tickMs - 1) / tickMs) * tickMs;
    }

    private static class ScheduledSend {
        private final Runnable sendNow;
        private final AtomicBoolean sent = new AtomicBoolean(false);

        private ScheduledSend(Runnable sendNow) {
            this.sendNow = sendNow;
        }

        private void send() {
            if (sent.compareAndSet(false, true)) {
                sendNow.run();
            }
        }
    }
}
