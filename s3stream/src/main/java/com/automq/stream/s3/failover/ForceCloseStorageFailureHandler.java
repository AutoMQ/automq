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

import com.automq.stream.s3.S3StreamClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Force to upload all data in memory to S3 and close the streams.
 */
public class ForceCloseStorageFailureHandler implements StorageFailureHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ForceCloseStorageFailureHandler.class);
    private final S3StreamClient streamClient;

    public ForceCloseStorageFailureHandler(S3StreamClient streamClient) {
        this.streamClient = streamClient;
    }

    @Override
    public void handle(Throwable ex) {
        LOGGER.error("Encounter storage fail, try force to close the streams", ex);
        this.streamClient.forceClose();
        LOGGER.info("Complete force to close the streams");
    }
}
