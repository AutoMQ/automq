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

package com.automq.stream.s3.failover;

import com.automq.stream.s3.wal.impl.block.BlockWALService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class FailoverTest {
    String path;
    FailoverFactory failoverFactory;
    WALRecover walRecover;
    Failover failover;

    @BeforeEach
    public void setup() throws IOException {
        path = "/tmp/" + System.currentTimeMillis() + "/failover_test_wal";
        failoverFactory = mock(FailoverFactory.class);
        walRecover = mock(WALRecover.class);
        failover = spy(new Failover(failoverFactory, walRecover));

        // write node id and epoch to the wal header
        BlockWALService wal = BlockWALService.builder(path, 1024 * 1024).nodeId(233).epoch(100).build();
        wal.start();
        wal.shutdownGracefully();
        when(failoverFactory.getWal(any())).thenAnswer(s ->
            BlockWALService.builder(path, 1024 * 1024).nodeId(233).epoch(100).build());
    }

    @AfterEach
    public void cleanup() throws IOException {
        Files.delete(Path.of(path));
    }

    @Test
    public void testNodeIdMismatch() {
        FailoverRequest request = createRequest(234, 100);
        ExecutionException executionException = Assertions.assertThrows(ExecutionException.class,
            () -> failover.failover(request).get(100, TimeUnit.SECONDS));
        Assertions.assertInstanceOf(IllegalArgumentException.class, executionException.getCause());
    }

    @Test
    public void testNodeEpochExpired() {
        FailoverRequest request = createRequest(233, 99);
        ExecutionException executionException = Assertions.assertThrows(ExecutionException.class,
            () -> failover.failover(request).get(100, TimeUnit.SECONDS));
        Assertions.assertInstanceOf(IllegalStateException.class, executionException.getCause());
    }

    @Test
    public void testAllMatch() throws ExecutionException, InterruptedException, TimeoutException {
        FailoverRequest request = createRequest(233, 100);
        FailoverResponse resp = failover.failover(request).get(100, TimeUnit.SECONDS);
        assertEquals(233, resp.getNodeId());
        assertEquals(100, resp.getEpoch());
    }

    @Test
    public void testNodeEpochGreaterThan() throws ExecutionException, InterruptedException, TimeoutException {
        FailoverRequest request = createRequest(233, 101);
        FailoverResponse resp = failover.failover(request).get(100, TimeUnit.SECONDS);
        assertEquals(233, resp.getNodeId());
        assertEquals(101, resp.getEpoch());
    }

    private FailoverRequest createRequest(int nodeId, long nodeEpoch) {
        FailoverRequest request = new FailoverRequest();
        request.setNodeId(nodeId);
        request.setNodeEpoch(nodeEpoch);
        request.setVolumeId("test_volume_id");
        request.setDevice(path);
        return request;
    }
}
