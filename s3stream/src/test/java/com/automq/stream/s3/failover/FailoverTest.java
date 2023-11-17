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

package com.automq.stream.s3.failover;

import com.automq.stream.s3.wal.BlockWALService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        BlockWALService wal = new BlockWALService.BlockWALServiceBuilder(path, 1024 * 1024).nodeId(233).epoch(100).build();
        wal.start();
        wal.shutdownGracefully();

        FailoverRequest request = new FailoverRequest();

        // node mismatch
        request.setNodeId(234);
        request.setDevice(path);
        request.setVolumeId("test_volume_id");

        boolean exceptionThrown = false;
        try {
            failover.failover(request).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                exceptionThrown = true;
            }
        }
        Assertions.assertTrue(exceptionThrown);

        // node match
        request.setNodeId(233);
        failover.failover(request).get(1, TimeUnit.SECONDS);

        ArgumentCaptor<FailoverRequest> ac = ArgumentCaptor.forClass(FailoverRequest.class);
        verify(failover, times(1)).complete(ac.capture());
        FailoverRequest req = ac.getValue();
        assertEquals(233, req.getNodeId());
        assertEquals("test_volume_id", req.getVolumeId());
    }

}
