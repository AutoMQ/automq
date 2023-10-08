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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
}
