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

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageFailureHandlerChain implements StorageFailureHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageFailureHandlerChain.class);
    private final List<StorageFailureHandler> handlers = new ArrayList<>();

    @Override
    public void handle(Throwable ex) {
        for (StorageFailureHandler handler : handlers) {
            try {
                handler.handle(ex);
            } catch (Throwable e) {
                LOGGER.error("{} Handle storage failure error", handler, e);
            }
        }
    }

    public void addHandler(StorageFailureHandler handler) {
        handlers.add(handler);
    }
}
