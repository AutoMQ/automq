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

package com.automq.stream.s3.failover;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class StorageFailureHandlerChain implements StorageFailureHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageFailureHandlerChain.class);
    private final List<StorageFailureHandler> handlers = new ArrayList<>();
    private final ExecutorService executorService = Threads.newFixedThreadPoolWithMonitor(1, "storage-failure-handler", true, LOGGER);

    @Override
    public void handle(Throwable ex) {
        // async handle storage failure to prevent blocking the WAL callback thread which may cause deadlock
        executorService.submit(() -> {
            for (StorageFailureHandler handler : handlers) {
                try {
                    handler.handle(ex);
                } catch (Throwable e) {
                    LOGGER.error("{} Handle storage failure error", handler, e);
                }
            }
        });
    }

    public void addHandler(StorageFailureHandler handler) {
        handlers.add(handler);
    }
}
