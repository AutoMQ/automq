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
