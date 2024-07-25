/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.common;

import com.automq.stream.utils.LogContext;
import org.slf4j.Logger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class AutoBalancerThreadFactory implements ThreadFactory {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    private final String name;
    private final boolean daemon;
    private final AtomicInteger id = new AtomicInteger(0);
    private final Logger logger;

    public AutoBalancerThreadFactory(String name) {
        this(name, true, null);
    }

    public AutoBalancerThreadFactory(String name, boolean daemon, Logger logger) {
        this.name = name;
        this.daemon = daemon;
        this.logger = logger == null ? LOGGER : logger;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name + "-" + id.getAndIncrement());
        t.setDaemon(daemon);
        t.setUncaughtExceptionHandler((t1, e) -> logger.error("Uncaught exception in " + t1.getName() + ": ", e));
        return t;
    }
}
