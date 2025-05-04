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

package kafka.autobalancer.services;

import kafka.autobalancer.common.AutoBalancerConstants;

import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractResumableService implements ResumableService {
    protected final Logger logger;
    protected final AtomicBoolean running = new AtomicBoolean(false);
    protected final AtomicBoolean shutdown = new AtomicBoolean(false);
    protected final AtomicInteger epoch = new AtomicInteger(0);

    public AbstractResumableService(LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[AbstractResumableService] ");
        }
        this.logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    }

    protected boolean isRunnable(int epoch) {
        return running.get() && this.epoch.get() == epoch;
    }

    @Override
    public final void run() {
        if (shutdown.get()) {
            logger.warn("Service is shutdown, cannot be running again.");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            logger.warn("Service is already running.");
            return;
        }
        epoch.incrementAndGet();
        doRun();
        logger.info("Service is running.");
    }

    @Override
    public final void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            logger.warn("Service is already shutdown.");
            return;
        }
        this.running.set(false);
        doShutdown();
        logger.info("Service shutdown.");
    }

    @Override
    public final void pause() {
        if (shutdown.get()) {
            logger.warn("Service is shutdown, cannot be paused.");
            return;
        }
        if (!running.compareAndSet(true, false)) {
            logger.warn("Service is already paused.");
            return;
        }
        doPause();
        logger.info("Service paused.");
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    public int currentEpoch() {
        return epoch.get();
    }

    protected abstract void doRun();
    protected abstract void doShutdown();
    protected abstract void doPause();
}
