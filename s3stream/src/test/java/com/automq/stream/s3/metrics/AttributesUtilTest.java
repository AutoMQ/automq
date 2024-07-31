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

package com.automq.stream.s3.metrics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AttributesUtilTest {

    @Test
    public void testGetObjectSizeBucket() {
        Assertions.assertEquals("16KB", AttributesUtils.getObjectBucketLabel(8 * 1024));
        Assertions.assertEquals("16KB", AttributesUtils.getObjectBucketLabel(16 * 1024));
        Assertions.assertEquals("32KB", AttributesUtils.getObjectBucketLabel(17 * 1024));
        Assertions.assertEquals("32KB", AttributesUtils.getObjectBucketLabel(32 * 1024));
        Assertions.assertEquals("64KB", AttributesUtils.getObjectBucketLabel(33 * 1024));
        Assertions.assertEquals("64KB", AttributesUtils.getObjectBucketLabel(64 * 1024));
        Assertions.assertEquals("128KB", AttributesUtils.getObjectBucketLabel(65 * 1024));
        Assertions.assertEquals("128KB", AttributesUtils.getObjectBucketLabel(128 * 1024));
        Assertions.assertEquals("256KB", AttributesUtils.getObjectBucketLabel(129 * 1024));
        Assertions.assertEquals("256KB", AttributesUtils.getObjectBucketLabel(256 * 1024));
        Assertions.assertEquals("512KB", AttributesUtils.getObjectBucketLabel(257 * 1024));
        Assertions.assertEquals("512KB", AttributesUtils.getObjectBucketLabel(512 * 1024));
        Assertions.assertEquals("1MB", AttributesUtils.getObjectBucketLabel(513 * 1024));
        Assertions.assertEquals("1MB", AttributesUtils.getObjectBucketLabel(1024 * 1024));
        Assertions.assertEquals("2MB", AttributesUtils.getObjectBucketLabel(1025 * 1024));
        Assertions.assertEquals("2MB", AttributesUtils.getObjectBucketLabel(2 * 1024 * 1024));
        Assertions.assertEquals("4MB", AttributesUtils.getObjectBucketLabel(2 * 1024 * 1024 + 1));
        Assertions.assertEquals("4MB", AttributesUtils.getObjectBucketLabel(4 * 1024 * 1024));
        Assertions.assertEquals("8MB", AttributesUtils.getObjectBucketLabel(4 * 1024 * 1024 + 1));
        Assertions.assertEquals("8MB", AttributesUtils.getObjectBucketLabel(8 * 1024 * 1024));
        Assertions.assertEquals("16MB", AttributesUtils.getObjectBucketLabel(8 * 1024 * 1024 + 1));
        Assertions.assertEquals("16MB", AttributesUtils.getObjectBucketLabel(16 * 1024 * 1024));
        Assertions.assertEquals("32MB", AttributesUtils.getObjectBucketLabel(16 * 1024 * 1024 + 1));
        Assertions.assertEquals("32MB", AttributesUtils.getObjectBucketLabel(32 * 1024 * 1024));
        Assertions.assertEquals("64MB", AttributesUtils.getObjectBucketLabel(32 * 1024 * 1024 + 1));
        Assertions.assertEquals("64MB", AttributesUtils.getObjectBucketLabel(64 * 1024 * 1024));
        Assertions.assertEquals("128MB", AttributesUtils.getObjectBucketLabel(64 * 1024 * 1024 + 1));
        Assertions.assertEquals("128MB", AttributesUtils.getObjectBucketLabel(128 * 1024 * 1024));
        Assertions.assertEquals("inf", AttributesUtils.getObjectBucketLabel(128 * 1024 * 1024 + 1));
        Assertions.assertEquals("inf", AttributesUtils.getObjectBucketLabel(1024 * 1024 * 1024));
    }
}
