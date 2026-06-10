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
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.ReservationService;
import com.automq.stream.utils.FutureUtil;

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
        String path = path(nodeId);
        ObjectStorage.ReadOptions readOptions = new ObjectStorage.ReadOptions().throttleStrategy(ThrottleStrategy.BYPASS).bucket(bucketId);
        ObjectStorage.WriteOptions writeOptions = new ObjectStorage.WriteOptions().throttleStrategy(ThrottleStrategy.BYPASS);
        CompletableFuture<Void> acquireCf = new CompletableFuture<>();
        objectStorage.rangeRead(readOptions, path, 0, S3_RESERVATION_OBJECT_LENGTH)
            .whenComplete((bytes, ex) -> {
                try {
                    CompletableFuture<Void> next = ex == null
                        ? acquireFromExistingReservation(bytes, writeOptions, path, nodeId, epoch, failover)
                        : acquireFromReadFailure(ex, writeOptions, path, nodeId, epoch, failover);
                    FutureUtil.propagate(next, acquireCf);
                } catch (Throwable t) {
                    acquireCf.completeExceptionally(t);
                }
            });
        return acquireCf;
    }

    private CompletableFuture<Void> acquireFromReadFailure(Throwable ex, ObjectStorage.WriteOptions options, String path,
        long nodeId, long epoch, boolean failover) {
        Throwable cause = FutureUtil.cause(ex);
        if (cause instanceof ObjectNotExistException) {
            return acquireFromMissingReservation(options, path, nodeId, epoch, failover);
        }
        return FutureUtil.failedFuture(cause);
    }

    private CompletableFuture<Void> acquireFromExistingReservation(ByteBuf bytes, ObjectStorage.WriteOptions options,
        String path, long nodeId, long epoch, boolean failover) {
        AcquireDecision decision = AcquireDecision.allow();
        try {
            decision = evaluateAcquireAction(parseReservation(nodeId, bytes), nodeId, epoch, failover);
        } catch (IllegalStateException ex) {
            LOGGER.warn("Overwrite invalid reservation object, nodeId={}, epoch={}, failover={}", nodeId, epoch, failover, ex);
        } finally {
            bytes.release();
        }
        if (!decision.allowed()) {
            return FutureUtil.failedFuture(new IllegalStateException(decision.rejectReason()));
        }
        return writeReservation(options, path, nodeId, epoch, failover);
    }

    private CompletableFuture<Void> acquireFromMissingReservation(ObjectStorage.WriteOptions options, String path,
        long nodeId, long epoch, boolean failover) {
        if (failover) {
            return FutureUtil.failedFuture(new IllegalStateException(
                String.format("Failover acquire cannot create missing reservation, nodeId=%s, epoch=%s", nodeId, epoch)));
        }
        return writeReservation(options, path, nodeId, epoch, false);
    }

    private AcquireDecision evaluateAcquireAction(ReservationRecord current, long nodeId, long epoch, boolean failover) {
        if (current.nodeId() != nodeId) {
            LOGGER.warn("Overwrite reservation object with mismatched node id, expect={}, actual={}", nodeId, current.nodeId());
            return AcquireDecision.allow();
        }

        // Existing reservation transition table:
        // - failover acquire: same/newer epoch -> WRITE, otherwise REJECT.
        // - normal acquire: same-epoch failover -> REJECT, same/newer epoch -> WRITE, otherwise REJECT.
        if (failover) {
            if (current.epoch() <= epoch) {
                return AcquireDecision.allow();
            }
            return AcquireDecision.reject(String.format(
                "Failover acquire rejected, nodeId=%s, currentEpoch=%s, requestEpoch=%s, currentFailover=%s",
                nodeId, current.epoch(), epoch, current.failover()));
        }

        if (current.epoch() == epoch) {
            if (current.failover()) {
                return AcquireDecision.reject(String.format(
                    "Normal acquire rejected, nodeId=%s, currentEpoch=%s, requestEpoch=%s, currentFailover=%s",
                    nodeId, current.epoch(), epoch, current.failover()));
            }
            return AcquireDecision.allow();
        }
        if (current.epoch() < epoch) {
            return AcquireDecision.allow();
        }
        return AcquireDecision.reject(String.format(
            "Normal acquire rejected, nodeId=%s, currentEpoch=%s, requestEpoch=%s, currentFailover=%s",
            nodeId, current.epoch(), epoch, current.failover()));
    }

    private CompletableFuture<Void> writeReservation(ObjectStorage.WriteOptions options, String path,
        long nodeId, long epoch, boolean failover) {
        // Use plain write for object-storage compatibility. This is not storage-level CAS;
        // ownership still depends on the caller verifying the final reservation content.
        return objectStorage.write(options.copy(), path, reservationBody(nodeId, epoch, failover)).thenApply(rst -> null);
    }

    private ReservationRecord parseReservation(long nodeId, ByteBuf bytes) {
        if (bytes.readableBytes() != S3_RESERVATION_OBJECT_LENGTH) {
            throw new IllegalStateException(String.format("Invalid reservation length, nodeId=%s, length=%s",
                nodeId, bytes.readableBytes()));
        }
        ByteBuf slice = bytes.slice();
        int magic = slice.readInt();
        if (magic != S3_RESERVATION_OBJECT_MAGIC_CODE) {
            throw new IllegalStateException(String.format("Invalid reservation magic code, nodeId=%s, magic=%s",
                nodeId, magic));
        }
        return new ReservationRecord(slice.readLong(), slice.readLong(), slice.readBoolean());
    }

    private ByteBuf reservationBody(long nodeId, long epoch, boolean failover) {
        ByteBuf target = ByteBufAlloc.byteBuffer(S3_RESERVATION_OBJECT_LENGTH);
        target.writeInt(S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(nodeId);
        target.writeLong(epoch);
        target.writeBoolean(failover);
        return target;
    }

    private record ReservationRecord(long nodeId, long epoch, boolean failover) {
    }

    private record AcquireDecision(boolean allowed, String rejectReason) {
        private static AcquireDecision allow() {
            return new AcquireDecision(true, null);
        }

        private static AcquireDecision reject(String rejectReason) {
            return new AcquireDecision(false, rejectReason);
        }
    }
}
