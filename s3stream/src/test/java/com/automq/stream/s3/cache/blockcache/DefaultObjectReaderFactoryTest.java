/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.MemoryObjectStorage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Verifies reader construction and logical cache identity when metadata supplies an explicit physical object key.
 */
@Tag("S3Unit")
public class DefaultObjectReaderFactoryTest {

    /**
     * Given published Archive metadata reusing an online object ID, verify a cache miss uses its physical Archive key
     * and subsequent lookup retains the existing logical object-ID identity.
     */
    @Test
    public void testArchivePhysicalKeyAndLogicalCacheIdentity() {
        long objectId = 101L;
        String archiveKey = "archive/7/0000000000000000020-0000000000000000010-101-536870912";
        DefaultObjectReaderFactory factory = new DefaultObjectReaderFactory(new MemoryObjectStorage());
        ObjectReader archiveReader = factory.get(metadata(objectId, archiveKey));

        ObjectReader sameLogicalObject = factory.get(metadata(objectId, "0/101"));

        assertEquals(archiveKey, archiveReader.objectKey());
        assertSame(archiveReader, sameLogicalObject);
        sameLogicalObject.release();
        archiveReader.release();
    }

    private static S3ObjectMetadata metadata(long objectId, String key) {
        return new S3ObjectMetadata(objectId, S3ObjectType.COMPOSITE,
            List.of(new StreamOffsetRange(7L, 10L, 20L)), 0L, 0L, 1L, objectId,
            ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).build().attributes(), key);
    }
}
