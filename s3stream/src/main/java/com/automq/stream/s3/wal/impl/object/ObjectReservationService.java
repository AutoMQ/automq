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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.Constants;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.ReservationService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ObjectReservationService implements ReservationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectReservationService.class);

    public static final int S3_RESERVATION_OBJECT_MAGIC_CODE = 0x12345678;
    public static final int S3_RESERVATION_OBJECT_LENGTH = 4 // magic code
                                                              + 8  // node id
                                                              + 8  // node epoch
                                                              + 1; // failover flag

    private final String clusterId;
    private final ObjectStorage objectStorage;
    private final short bucketId;

    private final ConcurrentMap<Long, String> nodeIdPathMap;

    public ObjectReservationService(String clusterId, ObjectStorage objectStorage, short bucketId) {
        this.clusterId = clusterId;
        this.objectStorage = objectStorage;
        this.nodeIdPathMap = new ConcurrentHashMap<>();
        this.bucketId = bucketId;
    }

    private String path(long nodeId) {
        return nodeIdPathMap.computeIfAbsent(nodeId, node -> "reservation/" + Constants.DEFAULT_NAMESPACE + clusterId + "/" + node);
    }

    // Visible for testing
    protected CompletableFuture<Boolean> verify(long nodeId, ByteBuf target) {
        ObjectStorage.ReadOptions options = new ObjectStorage.ReadOptions().throttleStrategy(ThrottleStrategy.BYPASS).bucket(bucketId);
        return objectStorage.rangeRead(options, path(nodeId), 0, S3_RESERVATION_OBJECT_LENGTH)
            .thenApply(bytes -> {
                try {
                    ByteBuf slice = bytes.slice();
                    slice.readInt();
                    if (bytes.readableBytes() != S3_RESERVATION_OBJECT_LENGTH) {
                        return false;
                    }
                    return bytes.equals(target);
                } finally {
                    bytes.release();
                    target.release();
                }
            })
            .exceptionally(e -> {
                LOGGER.error("Check reservation object failed:", e);
                return false;
            });
    }

    @Override
    public CompletableFuture<Boolean> verify(long nodeId, long epoch, boolean failover) {
        ByteBuf target = Unpooled.buffer(S3_RESERVATION_OBJECT_LENGTH);
        target.writeInt(S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(nodeId);
        target.writeLong(epoch);
        target.writeBoolean(failover);
        return verify(nodeId, target);
    }

    @Override
    public CompletableFuture<Void> acquire(long nodeId, long epoch, boolean failover) {
        LOGGER.info("Acquire permission for node: {}, epoch: {}, failover: {}", nodeId, epoch, failover);
        ByteBuf target = ByteBufAlloc.byteBuffer(S3_RESERVATION_OBJECT_LENGTH);
        target.writeInt(S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(nodeId);
        target.writeLong(epoch);
        target.writeBoolean(failover);
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions().throttleStrategy(ThrottleStrategy.BYPASS);
        return objectStorage.write(options, path(nodeId), target).thenApply(rst -> null);
    }
}
