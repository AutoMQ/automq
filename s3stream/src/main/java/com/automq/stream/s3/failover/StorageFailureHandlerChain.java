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
