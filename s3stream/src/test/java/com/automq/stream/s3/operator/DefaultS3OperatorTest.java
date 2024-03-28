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
import com.automq.stream.s3.metrics.TimerUtil;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.S3Error;

import static com.automq.stream.s3.operator.DefaultS3Operator.handleDeleteObjectsResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
                            .collect(Collectors.toList()))
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
    public void testMergeTask() {
        DefaultS3Operator.MergedReadTask mergedReadTask = new DefaultS3Operator.MergedReadTask(
            new DefaultS3Operator.ReadTask("obj0", 0, 1024, new CompletableFuture<>()), 0);
        boolean ret = mergedReadTask.tryMerge(new DefaultS3Operator.ReadTask("obj0", 1024, 2048, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new DefaultS3Operator.ReadTask("obj0", 2049, 3000, new CompletableFuture<>()));
        assertFalse(ret);
        assertEquals(0, mergedReadTask.dataSparsityRate);
        assertEquals(0, mergedReadTask.start);
        assertEquals(2048, mergedReadTask.end);
    }

    @Test
    public void testMergeTask2() {
        DefaultS3Operator.MergedReadTask mergedReadTask = new DefaultS3Operator.MergedReadTask(
            new DefaultS3Operator.ReadTask("obj0", 0, 1024, new CompletableFuture<>()), 0.5f);
        boolean ret = mergedReadTask.tryMerge(new DefaultS3Operator.ReadTask("obj0", 2048, 4096, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.25, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
        ret = mergedReadTask.tryMerge(new DefaultS3Operator.ReadTask("obj0", 1024, 1536, new CompletableFuture<>()));
        assertTrue(ret);
        assertEquals(0.125, mergedReadTask.dataSparsityRate, 0.01);
        assertEquals(0, mergedReadTask.start);
        assertEquals(4096, mergedReadTask.end);
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

        verify(operator, timeout(1000L).times(1)).mergedRangeRead(eq("obj0"), eq(0L), eq(4096L));
        verify(operator, timeout(1000L).times(1)).mergedRangeRead(eq("obj1"), eq(1024L), eq(3072L));
        verify(operator, timeout(1000L).times(1)).mergedRangeRead(eq("obj0"), eq(31457280L), eq(31461376L));
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

    private DeleteObjectsResponse geneDeleteObjectsResponse(List<String> successKeys, List<S3Error> errorKeys) {
        DeleteObjectsResponse response = mock(DeleteObjectsResponse.class);

        if (!successKeys.isEmpty()) {
            when(response.hasDeleted()).thenReturn(true);
            when(response.deleted()).thenReturn(successKeys.stream().map(k -> DeletedObject.builder()
                    .key(k)
                    .build()).collect(Collectors.toList()));
        }

        if (!errorKeys.isEmpty()) {
            when(response.hasErrors()).thenReturn(true);
            when(response.errors()).thenReturn(errorKeys);
        }

        return response;
    }

    private S3Error generateS3Error(String code, String message, String key) {
        return S3Error.builder().code(code).message(message).key(key).build();
    }

    @Test
    public void testHandleQuietModeDeleteObjectsResponse() {
        List<String> keys = IntStream.range(0, 10).mapToObj(i -> "deleteObj_" + i).sorted().collect(Collectors.toList());
        String mockBadKey = "deleteObj_0";
        List<String> expectNotBadKeys =  IntStream.range(1, 10).mapToObj(i -> "deleteObj_" + i).sorted().collect(Collectors.toList());
        TimerUtil util = new TimerUtil();


        // 1. normal case all delete success.
        DeleteObjectsResponse normalResponse = geneDeleteObjectsResponse(keys, Collections.emptyList());

        List<String> successKeys = handleDeleteObjectsResponse(keys, util, normalResponse, true);
        successKeys.sort(String::compareTo);

        assertEquals(successKeys, keys, "non-quiet deleteObjects should return all key success.");

        normalResponse = geneDeleteObjectsResponse(Collections.emptyList(), Collections.emptyList());
        successKeys = handleDeleteObjectsResponse(keys, util, normalResponse, false);
        successKeys.sort(String::compareTo);

        assertEquals(successKeys, keys, "quiet deleteObjects should return all key success.");


        // 2. normal case one key not exist.
        S3Error keyNotExistWithKey = generateS3Error("NoSuchKey", "mock NoSuchKey S3Error", mockBadKey);

        DeleteObjectsResponse responseWithOneKeyNotExistError = geneDeleteObjectsResponse(
                expectNotBadKeys,
                List.of(keyNotExistWithKey));

        successKeys = handleDeleteObjectsResponse(keys, util, responseWithOneKeyNotExistError, true);
        successKeys.sort(String::compareTo);

        assertEquals(successKeys, keys, "non-quiet deleteObjects should return all key success. " +
                "even with keys not exist.");

        responseWithOneKeyNotExistError = geneDeleteObjectsResponse(
                Collections.emptyList(),
                List.of(keyNotExistWithKey));
        successKeys = handleDeleteObjectsResponse(keys, util, responseWithOneKeyNotExistError, false);
        successKeys.sort(String::compareTo);

        assertEquals(successKeys, keys, "quiet deleteObjects should return all key success. " +
                "even with keys not exist.");


        // 3. normal case one key can't delete.
        S3Error keyErrorWithKey = generateS3Error("AccessDenied", "mock AccessDenied S3Error", mockBadKey);
        DeleteObjectsResponse oneObjectCanNotAccess = geneDeleteObjectsResponse(
                expectNotBadKeys,
                List.of(keyErrorWithKey));

        successKeys = handleDeleteObjectsResponse(keys, util, oneObjectCanNotAccess, true);
        successKeys.sort(String::compareTo);

        assertEquals(expectNotBadKeys, successKeys);
        assertFalse(successKeys.contains(mockBadKey), "non-quiet deleteObjects " +
                "should exclude one key not success delete.");

        DeleteObjectsResponse quietOneObjectCanNotAccess = geneDeleteObjectsResponse(
                Collections.emptyList(),
                List.of(keyErrorWithKey));

        successKeys = handleDeleteObjectsResponse(keys, util, quietOneObjectCanNotAccess, false);
        successKeys.sort(String::compareTo);

        assertEquals(expectNotBadKeys, successKeys);
        assertFalse(successKeys.contains(mockBadKey), "quiet deleteObjects should " +
                "exclude one key not success delete.");


        // 4. if the whole response with oneError without key.
        S3Error errorWithoutKey = generateS3Error("BadDigest", "mock BadDigest S3Error", "");
        DeleteObjectsResponse errorWithoutKeyResp = geneDeleteObjectsResponse(
                expectNotBadKeys,
                List.of(errorWithoutKey));

        successKeys = handleDeleteObjectsResponse(keys, util, errorWithoutKeyResp, true);
        assertEquals(expectNotBadKeys, successKeys, "non-quiet deleteObjects should return success delete keys " +
                "and ignore if unexpected S3Error without key received.");

        DeleteObjectsResponse quietRequestLevelError = geneDeleteObjectsResponse(
                Collections.emptyList(),
                List.of(errorWithoutKey));

        successKeys = handleDeleteObjectsResponse(keys, util, quietRequestLevelError, false);
        assertTrue(successKeys.isEmpty(), "quiet deleteObjects should return empty success delete keys " +
                "if unexpected S3Error without key received.");
    }
}
