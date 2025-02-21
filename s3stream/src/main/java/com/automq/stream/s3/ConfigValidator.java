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

package com.automq.stream.s3;

import io.netty.util.internal.PlatformDependent;

public class ConfigValidator {

    public static void validate(Config config) {
        long memoryLimit = ByteBufAlloc.getPolicy().isDirect() ? PlatformDependent.maxDirectMemory() : Runtime.getRuntime().maxMemory();
        long memoryRequired = config.blockCacheSize() + config.walCacheSize();
        if (memoryRequired > memoryLimit) {
            throw new IllegalArgumentException(String.format("blockCacheSize + walCacheSize size %s exceeds %s limit of %s", memoryRequired, ByteBufAlloc.getPolicy(), memoryLimit));
        }
        if (config.walUploadThreshold() > config.walCacheSize()) {
            throw new IllegalArgumentException(String.format("walUploadThreshold %s exceeds walCacheSize %s", config.walUploadThreshold(), config.walCacheSize()));
        }
    }

}
