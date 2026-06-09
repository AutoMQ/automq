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
import com.automq.stream.s3.exceptions.ObjectStorageConditionNotMetException;
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
    private static final int MAX_CONFLICT_REEVALUATION = 1;

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
        ObjectStorage.WriteOptions options = new ObjectStorage.WriteOptions().throttleStrategy(ThrottleStrategy.BYPASS);
        return acquire0(options, nodeId, epoch, failover, 0);
    }

    private CompletableFuture<Void> acquire0(ObjectStorage.WriteOptions options, long nodeId, long epoch,
        boolean failover, int conflictReevaluationCount) {
        String path = path(nodeId);
        return objectStorage.readWithMetadata(new ObjectStorage.ReadOptions().throttleStrategy(ThrottleStrategy.BYPASS).bucket(bucketId), path)
            .handle((result, ex) -> {
                if (ex != null) {
                    Throwable cause = FutureUtil.cause(ex);
                    if (cause instanceof ObjectNotExistException) {
                        return withConditionConflictReread(handleMissingReservation(options, path, nodeId, epoch, failover),
                            options, nodeId, epoch, failover, conflictReevaluationCount);
                    }
                    return FutureUtil.<Void>failedFuture(cause);
                }
                ReservationRecord current;
                try {
                    current = parseReservation(nodeId, result);
                } catch (Throwable parseEx) {
                    return FutureUtil.<Void>failedFuture(parseEx);
                } finally {
                    result.data().release();
                }
                AcquireDecision decision;
                try {
                    decision = decide(current, nodeId, epoch, failover);
                } catch (Throwable decisionEx) {
                    return FutureUtil.<Void>failedFuture(decisionEx);
                }
                return withConditionConflictReread(executeDecision(options, path, decision, nodeId, epoch, failover),
                    options, nodeId, epoch, failover, conflictReevaluationCount);
            }).thenCompose(cf -> cf);
    }

    private CompletableFuture<Void> withConditionConflictReread(CompletableFuture<Void> cf,
        ObjectStorage.WriteOptions options, long nodeId, long epoch, boolean failover, int conflictReevaluationCount) {
        return cf.exceptionallyCompose(ex -> {
            Throwable cause = FutureUtil.cause(ex);
            if (cause instanceof ObjectStorageConditionNotMetException
                && conflictReevaluationCount < MAX_CONFLICT_REEVALUATION) {
                LOGGER.info("Reservation conditional write conflict, node: {}, epoch: {}, failover: {}, retry: {}",
                    nodeId, epoch, failover, conflictReevaluationCount + 1);
                return acquire0(options.copy(), nodeId, epoch, failover, conflictReevaluationCount + 1);
            }
            return FutureUtil.failedFuture(cause);
        });
    }

    private CompletableFuture<Void> handleMissingReservation(ObjectStorage.WriteOptions options, String path,
        long nodeId, long epoch, boolean failover) {
        if (failover) {
            return FutureUtil.failedFuture(new IllegalStateException(
                String.format("Failover acquire cannot create missing reservation, nodeId=%s, epoch=%s", nodeId, epoch)));
        }
        return objectStorage.conditionalWrite(options.copy(), path, reservationBody(nodeId, epoch, false),
            new ObjectStorage.WriteCondition.IfAbsent()).thenApply(rst -> null);
    }

    private AcquireDecision decide(ReservationRecord current, long nodeId, long epoch, boolean failover) {
        if (current.nodeId != nodeId) {
            throw new IllegalStateException(
                String.format("Reservation node id mismatch, expect=%s, actual=%s", nodeId, current.nodeId));
        }
        if (failover) {
            if (current.epoch == epoch && current.failover) {
                return AcquireDecision.noop();
            }
            if (current.epoch == epoch && !current.failover) {
                return AcquireDecision.write(current.etag);
            }
            throw new IllegalStateException(
                String.format("Failover acquire rejected, nodeId=%s, currentEpoch=%s, requestEpoch=%s, currentFailover=%s",
                    nodeId, current.epoch, epoch, current.failover));
        }

        if (current.epoch == epoch && !current.failover) {
            return AcquireDecision.noop();
        }
        if (current.epoch < epoch) {
            return AcquireDecision.write(current.etag);
        }
        throw new IllegalStateException(
            String.format("Normal acquire rejected, nodeId=%s, currentEpoch=%s, requestEpoch=%s, currentFailover=%s",
                nodeId, current.epoch, epoch, current.failover));
    }

    private CompletableFuture<Void> executeDecision(ObjectStorage.WriteOptions options, String path,
        AcquireDecision decision, long nodeId, long epoch, boolean failover) {
        if (decision.noop) {
            return CompletableFuture.completedFuture(null);
        }
        return writeReservation(options, path, nodeId, epoch, failover, decision.etag);
    }

    private CompletableFuture<Void> writeReservation(ObjectStorage.WriteOptions options, String path,
        long nodeId, long epoch, boolean failover, ObjectStorage.Etag etag) {
        ByteBuf body = reservationBody(nodeId, epoch, failover);
        if (etag.value() != null) {
            return objectStorage.conditionalWrite(options.copy(), path, body,
                new ObjectStorage.WriteCondition.IfMatch(etag)).thenApply(rst -> null);
        }
        LOGGER.warn("Acquire reservation without object etag CAS protection, nodeId={}, epoch={}, failover={}",
            nodeId, epoch, failover);
        return objectStorage.write(options.copy(), path, body).thenApply(rst -> null);
    }

    private ReservationRecord parseReservation(long nodeId, ObjectStorage.ReadResult result) {
        ByteBuf bytes = result.data();
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
        long reservedNodeId = slice.readLong();
        long epoch = slice.readLong();
        boolean failover = slice.readBoolean();
        return new ReservationRecord(reservedNodeId, epoch, failover, result.metadata().etag());
    }

    private ByteBuf reservationBody(long nodeId, long epoch, boolean failover) {
        ByteBuf target = ByteBufAlloc.byteBuffer(S3_RESERVATION_OBJECT_LENGTH);
        target.writeInt(S3_RESERVATION_OBJECT_MAGIC_CODE);
        target.writeLong(nodeId);
        target.writeLong(epoch);
        target.writeBoolean(failover);
        return target;
    }

    private static class ReservationRecord {
        private final long nodeId;
        private final long epoch;
        private final boolean failover;
        private final ObjectStorage.Etag etag;

        private ReservationRecord(long nodeId, long epoch, boolean failover,
            ObjectStorage.Etag etag) {
            this.nodeId = nodeId;
            this.epoch = epoch;
            this.failover = failover;
            this.etag = etag;
        }
    }

    private static class AcquireDecision {
        private static final AcquireDecision NOOP = new AcquireDecision(true, new ObjectStorage.Etag(null));

        private final boolean noop;
        private final ObjectStorage.Etag etag;

        private AcquireDecision(boolean noop, ObjectStorage.Etag etag) {
            this.noop = noop;
            this.etag = etag;
        }

        private static AcquireDecision noop() {
            return NOOP;
        }

        private static AcquireDecision write(ObjectStorage.Etag etag) {
            return new AcquireDecision(false, etag);
        }
    }
}
