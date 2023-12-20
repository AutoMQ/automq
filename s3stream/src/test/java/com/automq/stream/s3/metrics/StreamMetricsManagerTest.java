/*
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

public class StreamMetricsManagerTest {

    @Test
    public void testGetObjectSizeBucket() {
        Assertions.assertEquals("16KB", S3StreamMetricsManager.getObjectBucketLabel(8 * 1024));
        Assertions.assertEquals("16KB", S3StreamMetricsManager.getObjectBucketLabel(16 * 1024));
        Assertions.assertEquals("32KB", S3StreamMetricsManager.getObjectBucketLabel(17 * 1024));
        Assertions.assertEquals("32KB", S3StreamMetricsManager.getObjectBucketLabel(32 * 1024));
        Assertions.assertEquals("64KB", S3StreamMetricsManager.getObjectBucketLabel(33 * 1024));
        Assertions.assertEquals("64KB", S3StreamMetricsManager.getObjectBucketLabel(64 * 1024));
        Assertions.assertEquals("128KB", S3StreamMetricsManager.getObjectBucketLabel(65 * 1024));
        Assertions.assertEquals("128KB", S3StreamMetricsManager.getObjectBucketLabel(128 * 1024));
        Assertions.assertEquals("256KB", S3StreamMetricsManager.getObjectBucketLabel(129 * 1024));
        Assertions.assertEquals("256KB", S3StreamMetricsManager.getObjectBucketLabel(256 * 1024));
        Assertions.assertEquals("512KB", S3StreamMetricsManager.getObjectBucketLabel(257 * 1024));
        Assertions.assertEquals("512KB", S3StreamMetricsManager.getObjectBucketLabel(512 * 1024));
        Assertions.assertEquals("1MB", S3StreamMetricsManager.getObjectBucketLabel(513 * 1024));
        Assertions.assertEquals("1MB", S3StreamMetricsManager.getObjectBucketLabel(1024 * 1024));
        Assertions.assertEquals("2MB", S3StreamMetricsManager.getObjectBucketLabel(1025 * 1024));
        Assertions.assertEquals("2MB", S3StreamMetricsManager.getObjectBucketLabel(2 * 1024 * 1024));
        Assertions.assertEquals("4MB", S3StreamMetricsManager.getObjectBucketLabel(2 * 1024 * 1024 + 1));
        Assertions.assertEquals("4MB", S3StreamMetricsManager.getObjectBucketLabel(4 * 1024 * 1024));
        Assertions.assertEquals("8MB", S3StreamMetricsManager.getObjectBucketLabel(4 * 1024 * 1024 + 1));
        Assertions.assertEquals("8MB", S3StreamMetricsManager.getObjectBucketLabel(8 * 1024 * 1024));
        Assertions.assertEquals("16MB", S3StreamMetricsManager.getObjectBucketLabel(8 * 1024 * 1024 + 1));
        Assertions.assertEquals("16MB", S3StreamMetricsManager.getObjectBucketLabel(16 * 1024 * 1024));
        Assertions.assertEquals("32MB", S3StreamMetricsManager.getObjectBucketLabel(16 * 1024 * 1024 + 1));
        Assertions.assertEquals("32MB", S3StreamMetricsManager.getObjectBucketLabel(32 * 1024 * 1024));
        Assertions.assertEquals("64MB", S3StreamMetricsManager.getObjectBucketLabel(32 * 1024 * 1024 + 1));
        Assertions.assertEquals("64MB", S3StreamMetricsManager.getObjectBucketLabel(64 * 1024 * 1024));
        Assertions.assertEquals("128MB", S3StreamMetricsManager.getObjectBucketLabel(64 * 1024 * 1024 + 1));
        Assertions.assertEquals("128MB", S3StreamMetricsManager.getObjectBucketLabel(128 * 1024 * 1024));
        Assertions.assertEquals("inf", S3StreamMetricsManager.getObjectBucketLabel(128 * 1024 * 1024 + 1));
        Assertions.assertEquals("inf", S3StreamMetricsManager.getObjectBucketLabel(1024 * 1024 * 1024));
    }
}
