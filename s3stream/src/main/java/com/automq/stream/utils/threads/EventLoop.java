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

package com.automq.stream.utils.threads;

import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public class EventLoop extends Thread implements Executor, ReferenceCounted {
    private final Logger logger;
    private final BlockingQueue<Runnable> tasks;
    private volatile boolean shutdown;
    private final CompletableFuture<Void> shutdownCf = new CompletableFuture<>();
    private final InnerRefCounted innerRefCounted = new InnerRefCounted();

    static final Runnable WAKEUP_TASK = new Runnable() {
        @Override
        public void run() {
        }
    };

    @SuppressWarnings("this-escape")
    public EventLoop(String name) {
        super(name);
        this.logger = new LogContext("[" + name + "]").logger(EventLoop.class);
        this.tasks = new LinkedBlockingQueue<>();
        start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Runnable task = tasks.poll(100, TimeUnit.MILLISECONDS);
                if (task == WAKEUP_TASK) {
                    task = null;
                }
                if (task == null) {
                    if (shutdown) {
                        shutdownCf.complete(null);
                        break;
                    } else {
                        continue;
                    }
                }
                try {
                    task.run();
                } catch (Throwable e) {
                    logger.error("Error running task", e);
                }
            } catch (InterruptedException e) {
                logger.info("EventLoop exit", e);
                break;
            }
        }
    }

    public synchronized CompletableFuture<Void> submit(Runnable task) {
        check();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        tasks.add(() -> {
            try {
                task.run();
                cf.complete(null);
            } catch (Throwable e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }

    @Override
    public synchronized void execute(Runnable task) {
        check();
        tasks.add(task);
    }

    public synchronized CompletableFuture<Void> shutdownGracefully() {
        shutdown = true;
        if (!shutdownCf.isDone() && tasks.isEmpty()) {
            tasks.add(WAKEUP_TASK);
        }
        return shutdownCf;
    }

    private void check() {
        if (shutdown) {
            throw new IllegalStateException("EventLoop is shutdown");
        }
    }

    @Override
    public int refCnt() {
        return innerRefCounted.refCnt();
    }

    @Override
    public ReferenceCounted retain() {
        return innerRefCounted.retain();
    }

    @Override
    public ReferenceCounted retain(int increment) {
        return innerRefCounted.retain(increment);
    }

    @Override
    public ReferenceCounted touch() {
        return innerRefCounted.touch();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return innerRefCounted.touch(hint);
    }

    @Override
    public boolean release() {
        return innerRefCounted.release();
    }

    @Override
    public boolean release(int decrement) {
        return innerRefCounted.release(decrement);
    }

    class InnerRefCounted extends AbstractReferenceCounted {

        @Override
        protected void deallocate() {
            shutdownGracefully();
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return EventLoop.this;
        }
    }
}
