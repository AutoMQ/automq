/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.services;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
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
    final public void run() {
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
    final public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            logger.warn("Service is already shutdown.");
            return;
        }
        this.running.set(false);
        doShutdown();
        logger.info("Service shutdown.");
    }

    @Override
    final public void pause() {
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
