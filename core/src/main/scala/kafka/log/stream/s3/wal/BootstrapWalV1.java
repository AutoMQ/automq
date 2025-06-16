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

package kafka.log.stream.s3.wal;

import kafka.log.stream.s3.node.NodeManager;

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.controller.stream.NodeMetadata;

import com.automq.stream.s3.exceptions.AutoMQException;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.WalFactory.BuildOptions;
import com.automq.stream.s3.wal.WalHandle;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.utils.IdURI;
import com.automq.stream.utils.Threads;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;



public class BootstrapWalV1 implements WriteAheadLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapWalV1.class);
    private final int nodeId;
    private final long nodeEpoch;
    private final String walConfigs;
    private final boolean failoverMode;

    private final WalFactory factory;
    private final NodeManager nodeManager;
    private final WalHandle walHandle;
    private final ExecutorService executor = Threads.newFixedThreadPoolWithMonitor(2, "bootstrap-wal", true, LOGGER);
    private volatile WriteAheadLog wal;
    private String currentWalConfigs;

    public BootstrapWalV1(int nodeId, long nodeEpoch, String walConfigs, boolean failoverMode,
        WalFactory factory, NodeManager nodeManager, WalHandle walHandle) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.walConfigs = walConfigs;
        this.failoverMode = failoverMode;
        this.factory = factory;
        this.nodeManager = nodeManager;
        this.walHandle = walHandle;

        try {
            // Init register node config if the node is the first time to start.
            NodeMetadata oldNodeMetadata = this.nodeManager.getNodeMetadata().get();
            if (StringUtils.isBlank(oldNodeMetadata.getWalConfig())) {
                currentWalConfigs = walConfigs;
                LOGGER.info("Init register nodeId={} nodeEpoch={} with WAL configs: {}", nodeId, nodeEpoch, currentWalConfigs);
                oldNodeMetadata = new NodeMetadata(nodeId, nodeEpoch, currentWalConfigs, Collections.emptyMap());
            } else {
                LOGGER.info("Get nodeId={} nodeEpoch={} old WAL configs: {} for recovery", nodeId, nodeEpoch, oldNodeMetadata);
            }

            // Build the WAL for recovery.
            if (nodeEpoch < oldNodeMetadata.getNodeEpoch()) {
                throw new AutoMQException("The node epoch is less than the current node epoch: " + nodeEpoch + " < " + oldNodeMetadata.getNodeEpoch());
            }

            currentWalConfigs = oldNodeMetadata.getWalConfig();
            this.wal = buildRecoverWal(currentWalConfigs, nodeEpoch).get();
        } catch (Throwable e) {
            throw new AutoMQException(e);
        }
    }

    @Override
    public WriteAheadLog start() throws IOException {
        return wal.start();
    }

    @Override
    public void shutdownGracefully() {
        wal.shutdownGracefully();
        ThreadUtils.shutdownExecutorServiceQuietly(executor, 10, TimeUnit.SECONDS);
    }

    @Override
    public WALMetadata metadata() {
        return wal.metadata();
    }

    @Override
    public CompletableFuture<AppendResult> append(TraceContext context, StreamRecordBatch streamRecordBatch) throws OverCapacityException {
        return wal.append(context, streamRecordBatch);
    }

    @Override
    public CompletableFuture<StreamRecordBatch> get(RecordOffset recordOffset) {
        return wal.get(recordOffset);
    }

    @Override
    public CompletableFuture<List<StreamRecordBatch>> get(RecordOffset startOffset, RecordOffset endOffset) {
        return wal.get(startOffset, endOffset);
    }

    @Override
    public RecordOffset confirmOffset() {
        return wal.confirmOffset();
    }

    @Override
    public Iterator<RecoverResult> recover() {
        return wal.recover();
    }

    @Override
    public CompletableFuture<Void> reset() {
        return CompletableFuture.runAsync(() -> {
            try {
                wal.reset().get();
                wal.shutdownGracefully();
                if (failoverMode) {
                    releasePermission(currentWalConfigs).get();
                    return;
                }
                this.currentWalConfigs = walConfigs;
                this.wal = buildWal(currentWalConfigs).get();
                wal.start();
                Iterator<RecoverResult> it = wal.recover();
                LOGGER.info("Register nodeId={} nodeEpoch={} with new WAL configs: {}", nodeId, nodeEpoch, currentWalConfigs);
                nodeManager.updateWal(currentWalConfigs).join();
                if (it.hasNext()) {
                    // Consider the following case:
                    // 1. Config: walConfigs = 0@replication://?walId=1&walId=2
                    // 2. When recovering, the walId=2 is temp failed, so we recover from walId=1 and reset it.
                    // 3. In reset phase, the walId=2 is alive, we generate a new walConfigs = 0@replication://?walId=1&walId=2
                    // 4. The walId=2 is not empty, we don't know whether the data is valid or not.
                    // 5. So we exit the process and try to reboot to recover.
                    throw new AutoMQException("[WARN] The WAL 'should be' empty, try reboot to recover");
                }
                wal.reset().get();
            } catch (Throwable e) {
                LOGGER.error("Reset WAL failed:", e);
                throw new AutoMQException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Void> trim(RecordOffset offset) {
        return wal.trim(offset);
    }

    private CompletableFuture<? extends WriteAheadLog> buildRecoverWal(String kraftWalConfigs, long oldNodeEpoch) {
        IdURI uri = IdURI.parse(kraftWalConfigs);
        CompletableFuture<Void> cf = walHandle
            .acquirePermission(nodeId, oldNodeEpoch, uri, new WalHandle.AcquirePermissionOptions().failoverMode(failoverMode));
        return cf.thenApplyAsync(nil -> factory.build(uri, BuildOptions.builder().nodeEpoch(oldNodeEpoch).openMode(failoverMode ? OpenMode.FAILOVER : OpenMode.READ_WRITE).build()), executor);
    }

    private CompletableFuture<? extends WriteAheadLog> buildWal(String kraftWalConfigs) {
        IdURI uri = IdURI.parse(kraftWalConfigs);
        WalHandle.AcquirePermissionOptions options = new WalHandle.AcquirePermissionOptions()
            .timeoutMs(Long.MAX_VALUE)
            .failoverMode(false);
        CompletableFuture<Void> cf = walHandle
            .acquirePermission(nodeId, nodeEpoch, uri, options);
        return cf.thenApplyAsync(nil -> factory.build(uri, BuildOptions.builder().nodeEpoch(nodeEpoch).openMode(OpenMode.READ_WRITE).build()), executor);
    }

    private CompletableFuture<Void> releasePermission(String kraftWalConfigs) {
        IdURI uri = IdURI.parse(kraftWalConfigs);
        return walHandle.releasePermission(uri, new WalHandle.ReleasePermissionOptions());
    }
}
