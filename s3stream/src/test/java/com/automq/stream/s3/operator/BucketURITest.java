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

package com.automq.stream.s3.operator;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

@Tag("S3Unit")
public class BucketURITest {

    @Test
    public void testParse_valid() {
        String bucketStr = "0@s3://bucket1?region=region1&k1=v1&k2=v2&k2=v22&endpoint=https://aws.amazon.com:444," +
                           "1@gs://bucket2?region=region2&endpoint=https://gcp," +
                           "-2@azblob://bucket3";
        List<BucketURI> buckets = BucketURI.parseBuckets(bucketStr);
        assertEquals((short) 0, buckets.get(0).bucketId());
        assertEquals("bucket1", buckets.get(0).bucket());
        assertEquals("region1", buckets.get(0).region());
        assertEquals("https://aws.amazon.com:444", buckets.get(0).endpoint());
        assertEquals("s3", buckets.get(0).protocol());
        assertEquals("v1", buckets.get(0).extensionString("k1", null));
        assertEquals(List.of("v2", "v22"), buckets.get(0).extensionStringList("k2"));
        assertEquals("", buckets.get(0).extensionString("k3", ""));
        assertEquals(Collections.emptyList(), buckets.get(0).extensionStringList("k3"));

        assertEquals((short) 1, buckets.get(1).bucketId());
        assertEquals("bucket2", buckets.get(1).bucket());
        assertEquals("region2", buckets.get(1).region());
        assertEquals("https://gcp", buckets.get(1).endpoint());
        assertEquals("gs", buckets.get(1).protocol());

        assertEquals((short) -2, buckets.get(2).bucketId());
        assertEquals("bucket3", buckets.get(2).bucket());
        assertEquals("", buckets.get(2).region());
        assertEquals("", buckets.get(2).endpoint());
        assertEquals("azblob", buckets.get(2).protocol());

        bucketStr = "0@file:///path/to/wal?type=raw&size=1G";
        BucketURI uri = BucketURI.parse(bucketStr);
        assertEquals("file", uri.protocol());
        assertEquals("/path/to/wal", uri.bucket());
        assertEquals("", uri.endpoint());
        assertEquals("", uri.region());
        assertEquals("raw", uri.extensionString("type"));
        assertEquals("1G", uri.extensionString("size"));
    }

    @Test
    public void testParse_invalid() {
        assertThrowsExactly(IllegalArgumentException.class, () -> BucketURI.parse("/path/to/wal"));
    }

    @Test
    public void testParseBucket() {
        String bucketStr = "0@s3://bucket-1?region=region1";
        BucketURI bucket = BucketURI.parse(bucketStr);
        assertEquals("bucket-1", bucket.bucket());

        bucketStr = "0@s3://bucket+1";
        bucket = BucketURI.parse(bucketStr);
        assertEquals("bucket+1", bucket.bucket());
    }
}
