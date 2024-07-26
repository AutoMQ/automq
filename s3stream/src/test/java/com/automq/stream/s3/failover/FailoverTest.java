/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.failover;

import com.automq.stream.s3.wal.impl.block.BlockWALService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    public void setup() {
        path = "/tmp/" + System.currentTimeMillis() + "/failover_test_wal";
        failoverFactory = mock(FailoverFactory.class);
        walRecover = mock(WALRecover.class);
        failover = spy(new Failover(failoverFactory, walRecover));
    }

    @AfterEach
    public void cleanup() throws IOException {
        Files.delete(Path.of(path));
    }

    @Test
    public void test() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        BlockWALService wal = BlockWALService.builder(path, 1024 * 1024).nodeId(233).epoch(100).build();
        wal.start();
        wal.shutdownGracefully();

        FailoverRequest request = new FailoverRequest();

        // node mismatch
        request.setNodeId(234);
        request.setDevice(path);
        request.setVolumeId("test_volume_id");

        when(failoverFactory.getWal(any())).thenReturn(BlockWALService.builder(path, 1024 * 1024).nodeId(233).epoch(100).build());

        boolean exceptionThrown = false;
        try {
            failover.failover(request).get(100, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                exceptionThrown = true;
            }
        }
        Assertions.assertTrue(exceptionThrown);

        // node match
        request.setNodeId(233);
        FailoverResponse resp = failover.failover(request).get(1, TimeUnit.SECONDS);
        assertEquals(233, resp.getNodeId());
    }

}
