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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class DefaultS3OperatorTest {

    private S3AsyncClient s3;
    private DefaultS3Operator operator;

    @BeforeEach
    void setUp() {
        s3 = mock(S3AsyncClient.class);
        operator = new DefaultS3Operator(s3, "test-bucket");
    }

    @AfterEach
    void tearDown() {
        operator.close();
    }

    @Test
    void testDeleteObjectsSuccess() {
        when(s3.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenAnswer(invocation -> {
                    DeleteObjectsRequest request = invocation.getArgument(0);
                    DeleteObjectsResponse response = DeleteObjectsResponse.builder()
                            .deleted(request.delete().objects().stream()
                                    .map(o -> DeletedObject.builder()
                                            .key(o.key())
                                            .build())
                                    .toList())
                            .build();
                    return CompletableFuture.completedFuture(response);
                });
        List<String> keys = List.of("test1", "test2");
        List<String> deleted = operator.delete(keys).join();
        assertEquals(keys, deleted);
    }

    @Test
    void testDeleteObjectsFail() {
        when(s3.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("test")));
        List<String> keys = List.of("test1", "test2");
        List<String> deleted = operator.delete(keys).join();
        assertEquals(Collections.emptyList(), deleted);
    }

    @Test
    void testMergeRead() throws ExecutionException, InterruptedException {
        operator = new DefaultS3Operator(s3, "test-bucket", true) {
            @Override
            CompletableFuture<ByteBuf> mergedRangeRead(String path, long start, long end) {
                return CompletableFuture.completedFuture(TestUtils.random((int) (end - start + 1)));
            }
        };
        operator = spy(operator);

        // obj0_0_1024 obj_1_1024_2048 obj_0_16776192_16777216 obj_0_2048_4096 obj_0_16777216_16778240
        CompletableFuture<ByteBuf> cf1 = operator.rangeRead("obj0", 0, 1024);
        CompletableFuture<ByteBuf> cf2 = operator.rangeRead("obj1", 1024, 3072);
        CompletableFuture<ByteBuf> cf3 = operator.rangeRead("obj0", 31457280, 31461376);
        CompletableFuture<ByteBuf> cf4 = operator.rangeRead("obj0", 2048, 4096);
        CompletableFuture<ByteBuf> cf5 = operator.rangeRead("obj0", 33554432, 33554944);

        operator.tryMergeRead();

        verify(operator, timeout(1000L).times(1)).mergedRangeRead(eq("obj0"), eq(0L), eq(31461376L));
        verify(operator, timeout(1000L).times(1)).mergedRangeRead(eq("obj1"), eq(1024L), eq(3072L));
        verify(operator, timeout(1000L).times(1)).mergedRangeRead(eq("obj0"), eq(33554432L), eq(33554944L));

        ByteBuf buf = cf1.get();
        assertEquals(1024, buf.readableBytes());
        buf.release();
        buf = cf2.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
        buf = cf3.get();
        assertEquals(4096, buf.readableBytes());
        buf.release();
        buf = cf4.get();
        assertEquals(2048, buf.readableBytes());
        buf.release();
        buf = cf5.get();
        assertEquals(512, buf.readableBytes());
        buf.release();
    }
}
