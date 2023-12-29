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

public class AttributesCacheTest {

    @Test
    public void testGetObjectSizeBucket() {
        AttributesCache cache = AttributesCache.INSTANCE;
        Assertions.assertEquals("16KB", cache.getObjectBucketLabel(8 * 1024));
        Assertions.assertEquals("16KB", cache.getObjectBucketLabel(16 * 1024));
        Assertions.assertEquals("32KB", cache.getObjectBucketLabel(17 * 1024));
        Assertions.assertEquals("32KB", cache.getObjectBucketLabel(32 * 1024));
        Assertions.assertEquals("64KB", cache.getObjectBucketLabel(33 * 1024));
        Assertions.assertEquals("64KB", cache.getObjectBucketLabel(64 * 1024));
        Assertions.assertEquals("128KB", cache.getObjectBucketLabel(65 * 1024));
        Assertions.assertEquals("128KB", cache.getObjectBucketLabel(128 * 1024));
        Assertions.assertEquals("256KB", cache.getObjectBucketLabel(129 * 1024));
        Assertions.assertEquals("256KB", cache.getObjectBucketLabel(256 * 1024));
        Assertions.assertEquals("512KB", cache.getObjectBucketLabel(257 * 1024));
        Assertions.assertEquals("512KB", cache.getObjectBucketLabel(512 * 1024));
        Assertions.assertEquals("1MB", cache.getObjectBucketLabel(513 * 1024));
        Assertions.assertEquals("1MB", cache.getObjectBucketLabel(1024 * 1024));
        Assertions.assertEquals("2MB", cache.getObjectBucketLabel(1025 * 1024));
        Assertions.assertEquals("2MB", cache.getObjectBucketLabel(2 * 1024 * 1024));
        Assertions.assertEquals("4MB", cache.getObjectBucketLabel(2 * 1024 * 1024 + 1));
        Assertions.assertEquals("4MB", cache.getObjectBucketLabel(4 * 1024 * 1024));
        Assertions.assertEquals("8MB", cache.getObjectBucketLabel(4 * 1024 * 1024 + 1));
        Assertions.assertEquals("8MB", cache.getObjectBucketLabel(8 * 1024 * 1024));
        Assertions.assertEquals("16MB", cache.getObjectBucketLabel(8 * 1024 * 1024 + 1));
        Assertions.assertEquals("16MB", cache.getObjectBucketLabel(16 * 1024 * 1024));
        Assertions.assertEquals("32MB", cache.getObjectBucketLabel(16 * 1024 * 1024 + 1));
        Assertions.assertEquals("32MB", cache.getObjectBucketLabel(32 * 1024 * 1024));
        Assertions.assertEquals("64MB", cache.getObjectBucketLabel(32 * 1024 * 1024 + 1));
        Assertions.assertEquals("64MB", cache.getObjectBucketLabel(64 * 1024 * 1024));
        Assertions.assertEquals("128MB", cache.getObjectBucketLabel(64 * 1024 * 1024 + 1));
        Assertions.assertEquals("128MB", cache.getObjectBucketLabel(128 * 1024 * 1024));
        Assertions.assertEquals("inf", cache.getObjectBucketLabel(128 * 1024 * 1024 + 1));
        Assertions.assertEquals("inf", cache.getObjectBucketLabel(1024 * 1024 * 1024));
    }
}
