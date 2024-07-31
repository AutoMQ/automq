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

package com.automq.stream.utils.threads;

import com.automq.stream.utils.LogContext;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class EventLoop extends Thread implements Executor {
    private final Logger logger;
    private BlockingQueue<Runnable> tasks;
    private boolean shutdown = false;
    private CompletableFuture<Void> shutdownCf = new CompletableFuture<>();

    @SuppressWarnings("this-escape")
    public EventLoop(String name) {
        super(name);
        this.logger = new LogContext("[" + name + "]").logger(EventLoop.class);
        this.tasks = new LinkedBlockingQueue<>();
        start();
    }

    public void run() {
        while (true) {
            try {
                Runnable task = tasks.poll(100, TimeUnit.MILLISECONDS);
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

    public CompletableFuture<Void> submit(Runnable task) {
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
    public void execute(Runnable task) {
        check();
        tasks.add(task);
    }

    public CompletableFuture<Void> shutdownGracefully() {
        shutdown = true;
        return shutdownCf;
    }

    private void check() {
        if (shutdown) {
            throw new IllegalStateException("EventLoop is shutdown");
        }
    }

}
