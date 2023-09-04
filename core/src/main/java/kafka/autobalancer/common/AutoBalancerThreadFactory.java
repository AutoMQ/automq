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

package kafka.autobalancer.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class AutoBalancerThreadFactory implements ThreadFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoBalancerThreadFactory.class);
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
