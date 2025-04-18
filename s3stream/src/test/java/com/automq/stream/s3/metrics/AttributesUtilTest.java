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
