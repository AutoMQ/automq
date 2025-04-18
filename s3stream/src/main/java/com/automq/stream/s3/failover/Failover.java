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

import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.WALNotInitializedException;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * To perform a Delta WAL failover, follow these steps:
 * 1. Ensure the old node stops writing to the delta WAL.
 * 2. Instruct the controller to reject all requests sent by the old node.
 * 3. Upload the delta WAL to S3.
 * 4. Lastly, close any streams that were opened by the old node and are currently active.
 */
public class Failover {
    private static final Logger LOGGER = LoggerFactory.getLogger(Failover.class);
    private final ExecutorService executor = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory("wal-failover-%d", true), LOGGER);
    private final FailoverFactory factory;
    private final WALRecover walRecover;

    public Failover(FailoverFactory factory, WALRecover walRecover) {
        this.factory = factory;
        this.walRecover = walRecover;
    }

    public CompletableFuture<FailoverResponse> failover(FailoverRequest request) {
        CompletableFuture<FailoverResponse> cf = new CompletableFuture<>();
        executor.submit(() -> FutureUtil.exec(() -> {
            try {
                cf.complete(new FailoverTask(request).failover());
            } catch (Throwable e) {
                LOGGER.error("failover {} fail", request, e);
                cf.completeExceptionally(e);
            }
        }, cf, LOGGER, "failover"));
        return cf;
    }

    class FailoverTask {
        private final FailoverRequest request;

        public FailoverTask(FailoverRequest request) {
            this.request = request;
        }

        public FailoverResponse failover() throws Throwable {
            LOGGER.info("failover start {}", request);
            int nodeId = request.getNodeId();
            long nodeEpoch = request.getNodeEpoch();

            FailoverResponse resp = new FailoverResponse();
            resp.setNodeId(nodeId);
            resp.setEpoch(nodeEpoch);

            // fence the device to ensure the old node stops writing to the delta WAL
            // recover WAL data and upload to S3
            WriteAheadLog wal = factory.getWal(request);
            try {
                wal.start();
            } catch (WALNotInitializedException ex) {
                LOGGER.info("fail over empty wal {}", request);
                return resp;
            }

            try {
                WALMetadata metadata = wal.metadata();
                if (nodeId != metadata.nodeId()) {
                    throw new IllegalArgumentException(String.format("nodeId mismatch, request=%s, wal=%s", request, metadata));
                }
                if (nodeEpoch < metadata.epoch()) {
                    throw new IllegalStateException(String.format("epoch mismatch, request=%s, wal=%s", request, metadata));
                }

                Logger taskLogger = new LogContext(String.format("[Failover nodeId=%s epoch=%s]", nodeId, nodeEpoch)).logger(FailoverTask.class);
                StreamManager streamManager = factory.getStreamManager(nodeId, nodeEpoch);
                ObjectManager objectManager = factory.getObjectManager(nodeId, nodeEpoch);
                LOGGER.info("failover recover {}", request);
                walRecover.recover(wal, streamManager, objectManager, taskLogger);
            } finally {
                wal.shutdownGracefully();
            }
            LOGGER.info("failover done {}", request);
            return resp;
        }

        @Override
        public String toString() {
            return "FailoverTask{" +
                "request=" + request +
                '}';
        }
    }

}
