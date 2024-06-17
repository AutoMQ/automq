/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;


@Tag("S3Unit")
public class AwsObjectStorageTest extends ObjectStorageTestBase {
    private S3AsyncClient s3;

    void doSetUp() {
        s3 = mock(S3AsyncClient.class);
    }

    @Test
    void testMergeRead() throws ExecutionException, InterruptedException {
        objectStorage = new AwsObjectStorage(s3, "test-bucket", true) {
            @Override
            CompletableFuture<ByteBuf> mergedRangeRead(String path, long start, long end) {
                return CompletableFuture.completedFuture(TestUtils.random((int) (end - start + 1)));
            }
        };
        testMergeRead0();
    }
}
